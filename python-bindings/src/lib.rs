use pyo3::prelude::*;
use rust_core::{DataLoader, DataLoaderError};
use std::path::PathBuf;

/// Pythonバインディング用のエラータイプ
#[derive(Debug)]
pub struct PyDataLoaderError {
    message: String,
}

impl std::fmt::Display for PyDataLoaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for PyDataLoaderError {}

impl From<DataLoaderError> for PyErr {
    fn from(err: DataLoaderError) -> Self {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{}", err))
    }
}

/// ゼロコピーデータローダー（Pythonバインディング）
#[pyclass]
pub struct PyDataLoader {
    loader: DataLoader,
}

#[pymethods]
impl PyDataLoader {
    /// 新しいデータローダーを作成
    #[new]
    fn new(shard_paths: Vec<String>) -> PyResult<Self> {
        let paths: Vec<PathBuf> = shard_paths.iter().map(|s| PathBuf::from(s)).collect();
        let loader = DataLoader::new(&paths)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{}", e)))?;
        Ok(Self { loader })
    }

    /// 指定されたインデックスのサンプルを取得（ゼロコピーでmemoryviewを返す）
    fn get_sample(&self, index: usize) -> PyResult<PyObject> {
        let sample = self.loader.get_sample(index)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{}", e)))?;

        // Pythonのmemoryviewオブジェクトを作成（ゼロコピー）
        Python::with_gil(|py| {
            let bytes = PyBytes::new(py, sample);
            Ok(bytes.into())
        })
    }

    /// 複数のサンプルを一度に取得
    fn get_batch(&self, indices: Vec<usize>) -> PyResult<Vec<PyObject>> {
        let samples = self.loader.get_batch(&indices)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{}", e)))?;

        Python::with_gil(|py| {
            let mut result = Vec::new();
            for sample in samples {
                let bytes = PyBytes::new(py, sample);
                result.push(bytes.into());
            }
            Ok(result)
        })
    }

    /// 次のN個のシャードをプリフェッチ
    fn prefetch_next(&mut self, count: usize) -> PyResult<()> {
        self.loader.prefetch_next(count)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{}", e)))?;
        Ok(())
    }

    /// プリフェッチの完了を待つ
    fn wait_prefetch(&mut self) -> PyResult<()> {
        self.loader.wait_prefetch()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{}", e)))?;
        Ok(())
    }

    /// 総サンプル数を取得
    fn total_samples(&self) -> usize {
        self.loader.total_samples()
    }

    /// シャード数を取得
    fn num_shards(&self) -> usize {
        self.loader.num_shards()
    }
}

/// Pythonモジュールの定義
#[pymodule]
fn zero_copy_loader(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyDataLoader>()?;
    Ok(())
}