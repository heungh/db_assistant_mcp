-- 데이터 타입 문제가 있는 CREATE TABLE 예시
CREATE TABLE financial_transactions (
    id int AUTO_INCREMENT PRIMARY KEY,
    account_number VARCHAR,
    transaction_amount MONEY,
    transaction_type varchar(20),
    currency_code char(10),
    exchange_rate float,
    transaction_fee double,
    processed_at DATETIME DEFAULT NOW(),
    is_verified BOOLEAN DEFAULT 'yes',
    notes longtext
);