-- 이미 존재하는 인덱스를 생성하려는 경우 (오류 예상)
CREATE INDEX idx_users_email ON users(email);