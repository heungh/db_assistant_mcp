-- 올바른 CREATE TABLE 예시 (표준 완전 준수)
CREATE TABLE user_profiles (
    id BIGINT AUTO_INCREMENT,
    user_id BIGINT NOT NULL,
    profile_image_url VARCHAR(500),
    bio_description TEXT,
    birth_date DATE,
    gender ENUM('M', 'F', 'O') DEFAULT 'O',
    country_code CHAR(2),
    timezone_offset INT DEFAULT 0,
    is_public BOOLEAN DEFAULT FALSE,
    last_login_at TIMESTAMP NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT pk_user_profiles PRIMARY KEY (id),
    CONSTRAINT fk_user_profiles_users FOREIGN KEY (user_id) REFERENCES users(id),
    CONSTRAINT uk_user_profiles_user_id UNIQUE (user_id)
) COMMENT '사용자 프로필 정보 테이블';