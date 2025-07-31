-- 문법 오류가 있는 CREATE TABLE 예시
CREATE TABLE shopping_carts (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    session_id VARCHAR(128),
    cart_status ENUM('active', 'abandoned', 'converted') DEFAULT 'active',
    total_items INT DEFAULT 0,
    total_amount DECIMAL(12,2) DEFAULT 0.00,
    expires_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT fk_shopping_carts_users FOREIGN KEY (user_id) REFERENCES users(id),
    CONSTRAINT ck_shopping_carts_total_items CHECK (total_items >= 0)
    -- 세미콜론 누락으로 문법 오류