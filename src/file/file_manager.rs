use super::{block::BlockId, page::Page};
use anyhow::Result;
use std::{
    collections::HashMap,
    fs::{create_dir_all, read_dir, File, OpenOptions},
    io::{Read as _, Seek as _, Write as _},
    path::PathBuf,
};

#[derive(Default)]
pub struct FileManager {
    pub db_dir: PathBuf,
    pub block_size: i32,
    pub is_new: bool,
    pub open_files: HashMap<String, File>,
}

impl FileManager {
    pub fn new(db_dir: impl Into<PathBuf>, block_size: i32) -> Result<Self> {
        let db_dir = db_dir.into();
        let is_new = !db_dir.exists();
        if is_new {
            create_dir_all(&db_dir)?;
        } else {
            for entry in read_dir(&db_dir)? {
                let entry = entry?;
                let path = entry.path();
                let name = entry.file_name();
                if path.is_file() && name.to_string_lossy().starts_with("temp") {
                    std::fs::remove_file(&path)?;
                }
            }
        }

        Ok(FileManager {
            db_dir,
            block_size,
            is_new,
            open_files: HashMap::new(),
        })
    }

    // TODO: thread safe
    pub fn read(&mut self, block: &BlockId, page: &mut Page) -> Result<()> {
        let block_size = self.block_size;
        let mut file = self.get_file(&block.filename)?;
        let offset = block.num * block_size;
        file.seek(std::io::SeekFrom::Start(offset as u64))?;
        _ = file.read(page.contents_mut())?;
        Ok(())
    }

    // TODO: thread safe
    pub fn write(&mut self, block: &BlockId, page: &mut Page) -> Result<()> {
        let block_size = self.block_size;
        let mut file = self.get_file(&block.filename)?;
        let offset = block.num * block_size;
        file.seek(std::io::SeekFrom::Start(offset as u64))?;
        file.write_all(page.contents())?;
        Ok(())
    }

    pub fn get_file<'a>(&'a mut self, filename: &'a str) -> Result<&'a File> {
        if self.open_files.contains_key(filename) {
            self.open_files
                .get(filename)
                .ok_or(anyhow::anyhow!("cannot open file {}", filename))
        } else {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(self.db_dir.join(filename))?;
            self.open_files.insert(filename.to_string(), file);
            Ok(self.open_files.get(filename).unwrap())
        }
    }

    /// append_block 指定したファイルに新しいブロックを追加して、そのブロックのIDを返す
    pub fn append_block(&mut self, filename: &str) -> Result<BlockId> {
        let block = BlockId::new(filename.to_string(), self.block_count(filename)? as i32);
        let offset = block.num * self.block_size;
        let bytes = vec![0; self.block_size as usize];
        let mut file = self.get_file(filename)?;
        file.seek(std::io::SeekFrom::Start(offset as u64))?;
        file.write_all(&bytes)?;
        Ok(block)
    }

    // length returns block count
    pub fn block_count(&mut self, filename: &str) -> Result<u64> {
        let file = self.get_file(filename)?;
        Ok(file.metadata()?.len() / self.block_size as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::remove_dir_all;
    use tempfile::tempdir;

    #[test]
    fn should_new_file_manager() {
        let tempdir = tempdir().unwrap();
        let path = tempdir.as_ref();
        let _ = remove_dir_all(path);
        let file_manager = FileManager::new(path, 32).unwrap();
        assert_eq!(file_manager.db_dir, PathBuf::from(path));
        assert_eq!(file_manager.block_size, 32);
        assert_eq!(file_manager.open_files.len(), 0);
        assert!(file_manager.is_new);
    }

    #[test]
    fn should_remove_temp_file() {
        let tempdir = tempdir().unwrap();
        let path = tempdir.as_ref();
        let tmpfile = tempdir.path().join("temp");
        let file = File::create(&tmpfile).unwrap();
        drop(file);
        FileManager::new(path, 32).unwrap();
        assert!(!tmpfile.exists());
    }

    #[test]
    fn should_can_get_new_file() {
        let tempdir = tempdir().unwrap();
        let path = tempdir.as_ref();
        let mut file_manager = FileManager::new(path, 32).unwrap();
        file_manager.get_file("test").unwrap();
        assert_eq!(file_manager.open_files.len(), 1);
        let file = PathBuf::from(path).join("test");
        let exists = file.exists();
        assert!(exists);
    }

    #[test]
    fn should_can_append_file() {
        let tempdir = tempdir().unwrap();
        let path = tempdir.as_ref();
        let mut file_manager = FileManager::new(path, 32).unwrap();
        let block = file_manager.append_block("test").unwrap();
        assert_eq!(block.num, 0);
        assert_eq!(block.filename, "test");
        let file = file_manager.get_file(&block.filename).unwrap();
        assert_eq!(
            file.metadata().unwrap().len(),
            file_manager.block_size as u64
        );
    }

    #[test]
    fn should_can_append_file_muitple_times() {
        let tempdir = tempdir().unwrap();
        let path = tempdir.as_ref();
        let mut file_manager = FileManager::new(path, 32).unwrap();
        let block = file_manager.append_block("test").unwrap();
        assert_eq!(block.num, 0);
        assert_eq!(block.filename, "test");
        let block = file_manager.append_block("test").unwrap();
        assert_eq!(block.num, 1);
        assert_eq!(block.filename, "test");
        let file = file_manager.get_file(&block.filename).unwrap();
        assert_eq!(
            file.metadata().unwrap().len(),
            file_manager.block_size as u64 * 2
        );
        assert!(file_manager.open_files.contains_key("test"));
    }

    #[test]
    fn should_write_and_read_page_to_file() {
        let tempdir = tempdir().unwrap();
        let path = tempdir.as_ref();
        let mut file_manager = FileManager::new(path, 32).unwrap();
        let block = file_manager.append_block("test").unwrap();
        let mut page = Page::new(file_manager.block_size);
        page.set_string(0, "hello");
        page.set_string(10, "world");
        file_manager.write(&block, &mut page).unwrap();
        let mut read_page = Page::new(32);
        file_manager.read(&block, &mut read_page).unwrap();
        assert_eq!(read_page.get_string(0), "hello");
        assert_eq!(read_page.get_string(10), "world");
    }
}
