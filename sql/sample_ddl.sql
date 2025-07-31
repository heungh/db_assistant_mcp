-- ========================================
-- DDL 샘플 테스트 파일
-- 스키마 검증 프로그램 테스트용
-- ========================================

-- 1. 올바른 CREATE TABLE 예시 (표준 준수)
CREATE TABLE users (
    id BIGINT AUTO_INCREMENT,
    username VARCHAR(50) NOT NULL,
    email_address VARCHAR(255) NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    phone_number VARCHAR(20),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT pk_users PRIMARY KEY (id),
    CONSTRAINT uk_users_username UNIQUE (username),
    CONSTRAINT uk_users_email UNIQUE (email_address)
) COMMENT '사용자 정보 테이블';

-- 2. 표준 위반 CREATE TABLE 예시 (CamelCase 명명)
CREATE TABLE UserAccount (
    ID int AUTO_INCREMENT,
    UserName varchar(50),
    EmailAddr varchar(255),
    CreateDate datetime,
    ModifyDate datetime,
    IsActive tinyint
);

-- 3. 문법 오류 CREATE TABLE 예시 (세미콜론 누락, 제약조건 문제)
CREATE TABLE order_items (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    order_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    quantity INT NOT NULL DEFAULT 1,
    unit_price DECIMAL(10,2) NOT NULL,
    total_amount DECIMAL(10,2) AS (quantity * unit_price) STORED,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (order_id) REFERENCES orders(id),
    FOREIGN KEY (product_id) REFERENCES products(id)
    -- 세미콜론 누락

-- 4. 데이터 타입 문제 CREATE TABLE 예시
CREATE TABLE products (
    id int,
    name VARCHAR,
    price MONEY,
    description longtext,
    category char(5),
    is_available BOOLEAN DEFAULT 'true',
    created_at DATETIME DEFAULT NOW()
);

-- 5. 올바른 ALTER TABLE 예시
ALTER TABLE users 
ADD COLUMN birth_date DATE,
ADD COLUMN gender ENUM('M', 'F', 'O') DEFAULT 'O',
MODIFY COLUMN email_address VARCHAR(300) NOT NULL;

-- 6. 문법 오류 ALTER TABLE 예시 (세미콜론 누락)
ALTER TABLE users 
ADD COLUMN middle_name VARCHAR(50),
ADD COLUMN nickname VARCHAR(100),
MODIFY phone_number VARCHAR(25)
-- 세미콜론 누락

-- 7. 올바른 CREATE INDEX 예시
CREATE INDEX idx_users_email ON users (email_address);
CREATE INDEX idx_users_created_at ON users (created_at);
CREATE UNIQUE INDEX uk_users_phone ON users (phone_number);

-- 8. 표준 위반 CREATE INDEX 예시 (명명 규칙 위반)
CREATE INDEX user_email_idx ON users (email_address);
CREATE INDEX email_index ON users (email_address);
CREATE UNIQUE INDEX unique_username ON users (username);

-- 9. 복합 문제 CREATE TABLE 예시 (여러 문제 동시 발생)
CREATE TABLE OrderDetail (
    OrderDetailID int,
    OrderID int,
    ProductID int,
    Qty int,
    Price float,
    Discount float DEFAULT 0,
    CreateUser varchar(50),
    CreateTime datetime
);

-- 10. 제약조건 누락 CREATE TABLE 예시
CREATE TABLE customer_order (
    id int AUTO_INCREMENT,
    customer_id int,
    order_date date,
    total_amount decimal(10,2),
    status varchar(20),
    notes text
);