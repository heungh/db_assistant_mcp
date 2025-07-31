-- 올바른 ALTER TABLE 예시
ALTER TABLE users 
ADD COLUMN middle_name VARCHAR(100),
ADD COLUMN preferred_language CHAR(2) DEFAULT 'en',
ADD COLUMN notification_preferences JSON,
MODIFY COLUMN phone_number VARCHAR(25),
ADD CONSTRAINT ck_users_preferred_language CHECK (preferred_language IN ('en', 'ko', 'ja', 'zh'));