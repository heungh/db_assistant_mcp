-- 인덱스 명명 규칙 위반 예시
CREATE INDEX user_email_idx ON users (email_address);
CREATE INDEX email_search ON users (email_address);
CREATE UNIQUE INDEX unique_phone_number ON users (phone_number);
CREATE INDEX UserNameIndex ON users (username);
CREATE INDEX ix_created ON users (created_at);