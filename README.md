# ZC-Loader

高速なゼロコピーストリーミングI/Oレイヤー。mmap + io_uring（Linux）ベースのデータローダーで、MLの学習・推論時のデータローディングを高速化します。

## 特徴

- **ゼロコピーI/O**: データを「読んで」「コピーして」「デコードして」でCPUが死ぬのを止める
- **mmap + io_uring**: Linuxの最新I/O機能を活用した高速データアクセス
- **シャード化バイナリ形式**: 効率的なデータ配置とランダムアクセス
- **順次プリフェッチ**: 次のデータを非同期で先読み
- **Pythonバインディング**: PyO3による薄いバインディング、NumPy/PyTorchと統合可能

## アーキテクチャ

```
Python Binding (PyO3)
    ↓
Rust Core
    ├─ ShardReader (シャード読み込み管理)
    ├─ MmapManager (メモリマップ管理)
    ├─ IoUringPrefetcher (io_uring非同期プリフェッチ)
    └─ ZeroCopyBuffer (ゼロコピーバッファ)
```

## ビルド

### Rust Core

```bash
cd rust-core
cargo build --release
```

### Python Bindings

```bash
cd python-bindings
# 開発モードでインストール
maturin develop

# または、wheelをビルド
maturin build --release
```

## 使用方法

### Python

```python
from zero_copy_loader import DataLoader, to_numpy
import numpy as np

# データローダーを作成
loader = DataLoader(["shard1.bin", "shard2.bin", "shard3.bin"])

# サンプルを取得（ゼロコピー）
sample = loader.get_sample(0)
array = to_numpy(sample, dtype=np.float32, shape=(224, 224, 3))

# バッチで取得
indices = [0, 1, 2, 3]
batch = loader.get_batch(indices)
arrays = [to_numpy(s, dtype=np.float32, shape=(224, 224, 3)) for s in batch]

# プリフェッチ
loader.prefetch_next(count=2)  # 次の2つのシャードを先読み
loader.wait_prefetch()  # 完了を待つ
```

### Rust

```rust
use rust_core::DataLoader;

let loader = DataLoader::new(&["shard1.bin", "shard2.bin"])?;
let sample = loader.get_sample(0)?;  // ゼロコピーで&[u8]を取得
```

## ベンチマーク

```bash
# Rustベンチマーク
cd rust-core
cargo run --release --bin bench_io

# Pythonベンチマーク
cd bench
python3 bench_python.py

# perfで詳細な測定
perf stat -e cache-misses,cpu-cycles,instructions cargo run --release --bin bench_io
```

## パフォーマンス目標

- **スループット**: 標準的な`read()`の3-10倍
- **CPU使用率**: コピー回数の削減により大幅に削減
- **レイテンシ**: mmapによる即座のアクセス

## プロジェクト構造

```
.
├── rust-core/          # Rustコアライブラリ
│   ├── src/
│   │   ├── lib.rs      # メインAPI
│   │   ├── format.rs   # バイナリ形式定義
│   │   ├── mmap.rs     # メモリマップ管理
│   │   ├── prefetch.rs # io_uringプリフェッチ
│   │   ├── reader.rs   # シャード読み込み
│   │   └── buffer.rs    # ゼロコピーバッファ
│   └── src/bin/
│       └── bench_io.rs  # ベンチマーク
├── python-bindings/    # Pythonバインディング
│   ├── src/lib.rs      # PyO3バインディング
│   └── python/         # Pythonラッパー
├── bench/              # ベンチマークスクリプト
└── docs/               # ドキュメント
```

## ライセンス

[ライセンスを指定]

## 貢献

プルリクエストを歓迎します！

## 参考資料

- [io_uring documentation](https://kernel.dk/io_uring.pdf)
- [Zero-Copy I/O techniques](https://www.kernel.org/doc/html/latest/networking/msg_zerocopy.html)
- [WebDataset format](https://github.com/webdataset/webdataset)
