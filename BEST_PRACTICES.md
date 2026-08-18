# Go — Best Practices untuk `ledger-service`

> Diringkas dari dokumentasi resmi Go (go.dev/doc/modules/layout, "Server project") pada 2026-08-18, sebelum skeleton project dibuat.

## Struktur folder (layout resmi untuk server project)

Go tidak mewajibkan struktur tertentu, tapi dokumentasi resmi merekomendasikan pola ini untuk project server (bukan library yang di-import orang lain):

```
ledger-service/
  go.mod
  cmd/
    ledger-service/
      main.go        # entry point, hanya wiring — tidak ada logic bisnis
  internal/
    ledger/           # domain logic: double-entry, validasi saldo
    postgres/          # akses database, implementasi repository
    httpapi/            # HTTP handler (REST) untuk dipanggil BFF/Payment Gateway
```

**Kenapa `internal/`:** paket di dalam `internal/` tidak bisa di-import module lain — cocok untuk server yang tidak dimaksudkan jadi library publik, dan bebas di-refactor tanpa breaking change bagi pihak luar.

**Kenapa `cmd/`:** memisahkan entry point (`main.go`) dari logic. Kalau nanti butuh binary kedua (mis. `cmd/migrate/` untuk migrasi database), pola ini sudah siap tanpa restrukturisasi.

## Konvensi lain yang dipegang di skeleton ini

- **Error wrapping:** gunakan `fmt.Errorf("...: %w", err)` supaya caller bisa `errors.Is`/`errors.As`, terutama penting untuk membedakan error "saldo tidak cukup" (business error) vs error koneksi database (infra error).
- **Context propagation:** setiap fungsi yang menyentuh I/O (DB, HTTP) menerima `context.Context` sebagai parameter pertama — perlu untuk cancellation & request timeout di jalur transaksi kritis.
- **Dependency lewat constructor, bukan global state:** `internal/ledger` menerima interface repository lewat constructor (`NewService(repo Repository)`), bukan import langsung ke `internal/postgres` — memudahkan test dengan mock/in-memory repo.
- **Table-driven tests:** konvensi standar Go untuk unit test, terutama penting di sini untuk menguji berbagai skenario double-entry (saldo cukup, saldo kurang, transaksi duplikat/idempotency).
- **`gofmt` & `go vet`:** wajib bersih sebelum commit — tidak ada gaya formatting custom.

## Yang sengaja belum dipakai di skeleton awal

Framework HTTP (Gin/Echo/Chi) dan ORM belum dipilih — `net/http` standar library dan `database/sql` + driver `pgx` cukup untuk skeleton awal. Tambahkan dependency eksternal hanya kalau kebutuhannya jelas (mis. routing kompleks baru butuh Chi).
