-- 존재하지 않는 컬럼에 인덱스를 생성하려는 경우 (오류 예상)
CREATE INDEX idx_users_nonexistent ON users(nonexistent_column);