# Design: ORC Column Encryption (Read Path) for the Velox Backend

Status: **Proposal / design — Phase 1 code survey complete, no implementation yet.**
Scope: **Read/decrypt only.** KMS model follows the vivo/StarRocks article: the (encrypted)
master key lives in Hive table properties, is decrypted by a KMS service on the JVM side, and
the plaintext master key is passed to native, which unwraps the per-stripe local keys and
decrypts the streams.

References:
- vivo article *“StarRocks 读取 ORC 加密文件”* — <https://www.cnblogs.com/vivotech/p/18795964>
- Apache ORC encryption spec — <https://orc.apache.org/specification/ORCv1/#encryption>

All file:line references below are in the vendored Velox tree at
`ep/build-velox/build/velox_ep/` (branch/tag `dft-2026_06_06`, commit `9c2a5b332`).

---

## 0. Phase 1 survey — what already exists in Velox (the big news)

Velox’s ORC reader is **not** a separate implementation: `OrcReaderFactory::createReader`
just returns `DwrfReader::create(...)` (`velox/dwio/orc/reader/OrcReader.h:31`). ORC and
DWRF share one reader, distinguished by `FileFormat` and by which protobuf
(`orc_proto` vs `dwrf_proto`) backs the `FooterWrapper`. **So the encryption work lives in
`velox/dwio/dwrf/`, not `velox/dwio/orc/`.**

Crucially, most of the scaffolding the article had to hand-write into StarRocks’ ORC C++ is
**already present** in Velox:

| Capability | Status | Location |
|---|---|---|
| ORC encryption **proto schema** (full Apache ORC format) | ✅ present | `velox/dwio/dwrf/proto/orc_proto.proto` |
| — `Footer.encryption`, `Encryption{key, variants, keyProvider}` | ✅ | orc_proto.proto:373, 332, 301, 309, 323 |
| — `EncryptionAlgorithm` (incl. `AES_CTR_128`) | ✅ | orc_proto.proto:279 |
| — `StripeInformation.encryptStripeId / encryptedLocalKeys` | ✅ | orc_proto.proto:249, 252 |
| — `StripeFooter.encryption` (`StripeEncryptionVariant`) | ✅ | orc_proto.proto:183, 164 |
| — `Stream.Kind ENCRYPTED_INDEX / ENCRYPTED_DATA` + IV stream | ✅ | orc_proto.proto:135, 136, 140 |
| `Decrypter` / `DecrypterFactory` abstraction | ✅ present | `velox/dwio/common/encryption/Encryption.h` |
| Factory injection into the reader | ✅ present | `Options.h:676` `setDecrypterFactory` → `ReaderBase.cpp:206` |
| `DecryptionHandler` + node→provider mapping | ✅ present | `velox/dwio/dwrf/common/Decryption.{h,cpp}`, `EncryptionCommon.h` |
| Decrypt-then-decompress stream pipeline | ✅ present | `PagedInputStream.cpp:168-178` (decrypt whole page, then decompress) |
| Per-node decrypter injection during stream read | ✅ present | `StripeStream.cpp:347-351` (`createDecompressedStream(..., getDecrypter(node), ...)`) |
| Stripe-footer decryption | ✅ present (DWRF only) | `StripeStream.cpp:227-262` |
| **DWRF** column encryption (full read impl) | ✅ present | `Decryption.cpp:67-124` (encryptionGroups model) |

### The gaps (what is actually missing for ORC)

1. **ORC footer is never parsed for encryption.** `DecryptionHandler::create(FooterWrapper)`
   returns an **empty handler** for any non-DWRF format
   (`Decryption.cpp:62-64`):

   ```cpp
   return footer.format() == DwrfFormat::kDwrf
       ? create(*footer.getDwrfPtr(), factory)
       : std::make_unique<DecryptionHandler>();   // ← ORC: no decryption at all
   ```

   The existing `create(proto::Footer&)` implements DWRF’s **encryptionGroups** model
   (`encryptiongroups`, `keymetadata`, `group.nodes`) — structurally different from ORC’s
   **variants** model (`Encryption.variants`, `EncryptionVariant.root/key/encryptedKey`,
   per-stripe `StripeInformation.encryptedLocalKeys`). Needs a new ORC code path.

2. **No AES-CTR decrypter with ORC IV derivation.** `Decrypter::decrypt(std::string_view)`
   (`Encryption.h`) takes a single byte range and **no IV / column / stream / offset
   context**. DWRF’s `CryptoService` decrypter decrypts a whole blob without positional IV.
   ORC requires AES-CTR with a 16-byte IV derived from `(columnId, streamKind, stripeId,
   blockCounter)`, where the counter advances by byte offset. This is the **biggest interface
   change**: either extend the `Decrypter` contract to receive an IV/position context, or
   thread that context through `PagedInputStream` (`PagedInputStream.cpp:168`) to the cipher.

3. **No local-key unwrap.** Must decrypt `EncryptionVariant.encryptedKey` (footer key) and
   per-stripe `encryptedLocalKeys` with the master key to obtain the plaintext DEKs (the
   article’s statKey / dataKey).

4. **KeyProvider type.** `EncryptionProvider` enum has only `{Unknown, CryptoService}`
   (`Encryption.h`). ORC carries a `keyProvider` (KMS in the article) — add a provider kind
   and map `fromProto(orc_proto KeyProviderKind)`.

5. **Encrypted statistics.** Encrypted file/stripe statistics must be decrypted or predicate
   pushdown / pruning silently degrades on encrypted columns.

6. **Stripe-footer ORC variants.** `StripeStream.cpp:227-262` decrypts DWRF
   `encryptionGroups`; the ORC `StripeFooter.encryption` (`StripeEncryptionVariant`) path is
   not handled.

**Bottom line:** this is an *extension of an existing framework*, not a from-scratch port.
The proto, the decrypter abstraction, the factory-injection path, and the
decrypt-before-decompress stream pipeline are all in place.

---

## 1. Two tracks

- **Track V (Velox `dwio/dwrf`):** close gaps 1–6 above. The bulk of the effort, but on top
  of existing scaffolding. Carried as a patch under `ep/build-velox/src/*.patch` and/or
  upstreamed to Velox.
- **Track G (Gluten, this repo):** a `DecrypterFactory` that implements the article’s KMS
  scheme, plus Substrait/config plumbing to carry the plaintext master key JVM→native.

Read path (unchanged structurally — we only fill in the ORC branch):

```
Spark plan → Scala ORC scan exec → Substrait ReadRel.LocalFiles (OrcReadOptions, properties)
  → VeloxPlanConverter.cc (Substrait → SplitInfo, kOrc)        cpp/velox/.../VeloxPlanConverter.cc:182
  → SubstraitToVeloxPlan.cc (HiveTableHandle / scan node)
  → WholeStageResultIterator.cc (HiveConnectorSplit per file)  cpp/velox/.../WholeStageResultIterator.cc:253
  → Velox HiveConnector → DwrfReader (ORC)
      → ReaderBase: DecryptionHandler::create(footer, decrypterFactory)   ReaderBase.cpp:206
      → StripeStream: getDecrypter(node) → createDecompressedStream        StripeStream.cpp:347
      → PagedInputStream: decrypt(page) then decompress                    PagedInputStream.cpp:168
```

---

## 2. Track V — concrete change list (Velox `dwio/dwrf`)

1. **`Decryption.cpp` — add ORC footer parsing.** Replace the empty-handler branch
   (`Decryption.cpp:62-64`) with `create(const proto::orc::Footer&, factory)` that:
   - reads `Footer.encryption.variants`, maps each variant’s `root` subtree to a provider
     index via the existing `populateNodeMaps` machinery (reused as-is);
   - records `EncryptionKey` list + `keyProvider`;
   - for the footer key, unwraps `EncryptionVariant.encryptedKey` with the master key;
   - stores per-variant state needed to later unwrap per-stripe `encryptedLocalKeys`.

2. **New `OrcAesCtrDecrypter`** (impl of `dwio::common::encryption::Decrypter`): AES-128/256-CTR
   via OpenSSL (already a Velox dep — see `docs/get-started/VeloxDynamicOpenSSL.md`). Needs IV
   context (gap 2). Recommended: introduce an overload
   `decrypt(std::string_view, const StreamIV&)` (or a stateful decrypter bound per stream)
   and pass `(columnId, streamKind, stripeId, byteOffset)` from `PagedInputStream`. Keep the
   old single-arg `decrypt` for DWRF.

3. **Per-stripe local-key unwrap.** When reading each stripe, unwrap
   `StripeInformation.encryptedLocalKeys[variant]` (and honor `encryptStripeId`) into the
   stripe’s DEK; bind it to the per-stream decrypter.

4. **`StripeStream.cpp` — handle ORC `StripeFooter.encryption`** (`StripeEncryptionVariant`)
   alongside the existing DWRF `encryptionGroups` path (227-262).

5. **Decrypt encrypted statistics** (file + stripe) so pruning works on encrypted columns.

6. **`EncryptionProvider` enum + `fromProto`** for ORC’s KeyProvider kinds.

Tests (Velox side): read ORC files produced by the Apache ORC **Java** writer with
encryption — cross-implementation compatibility is the real correctness bar. A
`LocalKeyDecrypterFactory` (master key supplied directly) keeps tests offline; cf. the
existing `velox/dwio/common/encryption/TestProvider.h`.

---

## 3. Track G — Gluten changes (this repo)

### 3.1 Substrait proto (`gluten-substrait/.../substrait/proto/substrait/algebra.proto`)

`OrcReadOptions {}` is empty today (algebra.proto:151). Extend:

```proto
message OrcReadOptions {
  message Encryption {
    string key_name = 1;     // ORC encryption key name (Footer.EncryptionKey)
    bytes  master_key = 2;   // plaintext master key, already decrypted by the JVM KMS client
    string algorithm = 3;    // e.g. "AES_CTR_128"
  }
  Encryption encryption = 1; // absent => unencrypted (unchanged)
}
```

Update per `docs/developers/SubstraitModifications.md`. **Exclude `master_key` from
query-trace serialization** (`docs/developers/QueryTrace.md`) and from plan dumps/logs.

### 3.2 Native: build the `DecrypterFactory` and inject it

- `VeloxPlanConverter.cc` (`SubstraitFileFormatCase::kOrc`, ~line 182): read
  `OrcReadOptions.Encryption` onto `SplitInfo` (add `std::optional<OrcEncryptionInfo>` next to
  `properties` in `SubstraitToVeloxPlan.h:66`).
- Build a `dwio::common::encryption::DecrypterFactory` from that info (holds the plaintext
  master key; creates `OrcAesCtrDecrypter`s that unwrap local keys) and set it via
  `ReaderOptions::setDecrypterFactory` (`Options.h:676`) on the ORC reader options used by the
  Hive connector. (Channel for the key: per-split `customSplitInfo` at
  `WholeStageResultIterator.cc:261/263`, or connector session config via
  `ConfigExtractor.cc:244`. Recommend per-split for multi-table queries.)

### 3.3 JVM KMS client + config (matches the article)

```scala
trait GlutenKmsClient {
  def decryptMasterKey(keyName: String, encryptedMasterKey: Array[Byte]): Array[Byte]
}
```

- Read encrypted master key + key name from the table’s Hive `tblproperties` / serde props.
- Decrypt once on driver/executor JVM; attach plaintext key to the split’s `OrcReadOptions`.
- Pluggable via `spark.gluten.sql.columnar.backend.velox.orc.encryption.kmsClass`; ship a
  `LocalStaticKmsClient` for tests. Config keys live next to `VELOX_ORC_SCAN_ENABLED` /
  `ORC_USE_COLUMN_NAMES` in `backends-velox/.../config/VeloxConfig.scala`.
- **Fallback:** if encryption is enabled but the Velox build lacks ORC decryption or the KMS
  class is missing, fall back to vanilla Spark ORC scan (its Java reader supports encryption)
  rather than failing.

---

## 4. Phased plan

- **Phase 1 — survey (done).** Findings above.
- **Phase 2 — Velox ORC footer parsing + local-key unwrap.** Gap 1, 3, 6; reuse
  `EncryptionHandlerBase`/`populateNodeMaps`. Unit-test handler construction from a
  Java-written encrypted ORC footer.
- **Phase 3 — Velox AES-CTR decrypter + IV plumbing.** Gap 2, 4; the interface change.
  End-to-end native read of an encrypted ORC file with a local (test) key.
- **Phase 4 — Gluten plumbing.** Proto field, `SplitInfo`/`WholeStageResultIterator`/
  `setDecrypterFactory`, config keys, `LocalStaticKmsClient`.
- **Phase 5 — KMS integration + stats decryption + tests.** Gap 5; `gluten-ut` E2E vs vanilla
  Spark; fallback + wrong-key negative tests; docs in `docs/velox-configuration.md`.

## 5. Risks / open questions

- **Decrypter interface change** (gap 2) touches the shared DWRF path — keep DWRF behavior
  byte-for-byte unchanged; add ORC context as an additive overload.
- **Upstream vs patch:** land in Velox upstream or carry under `ep/build-velox/src/`? Affects
  maintenance and the `Decrypter` interface change review.
- **Masking variants** (ORC unauthorized-reader masking) — out of scope initially; document
  as unsupported.
- **AES-256** (`AES_CTR_256`) — support both via the `algorithm` field.
- **Key material in query trace / logs** — must scrub (see 3.1).
