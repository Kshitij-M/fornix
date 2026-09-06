// Package credentials stores opaque local credential tokens separately from
// non-secret Fornix profile metadata. Configuration and durable records carry
// validated references; callers resolve the corresponding secret only at the
// operation boundary that needs it.
//
// Secret values are never JSON/text marshalable and always render as a fixed
// redaction marker. The file fallback stores raw bytes in owner-only files,
// publishes replacements atomically, and shares the profile-wide process lock.
// It is a local fallback, not a control-plane credential authority or a
// replacement for an operating-system credential service.
package credentials
