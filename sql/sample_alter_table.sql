ALTER TABLE users 
ADD COLUMN phone VARCHAR(20),
ADD INDEX idx_username (username);