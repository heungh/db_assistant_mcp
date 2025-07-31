-- 명명 규칙 위반 CREATE TABLE 예시
CREATE TABLE ProductCategory (
    CategoryID int AUTO_INCREMENT PRIMARY KEY,
    CategoryName varchar(100) NOT NULL,
    ParentCategoryID int,
    CategoryDesc text,
    IsActive tinyint DEFAULT 1,
    CreateDate datetime DEFAULT CURRENT_TIMESTAMP,
    UpdateDate datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);