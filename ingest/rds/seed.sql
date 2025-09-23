-- Sample RDS database schema and data for CDC demonstration
-- This creates a simple users table with sample data

-- Create database if not exists
CREATE DATABASE IF NOT EXISTS interview_db;
USE interview_db;

-- Create users table
CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY AUTO_INCREMENT,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    phone VARCHAR(20),
    address VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    country VARCHAR(50) DEFAULT 'USA',
    zip_code VARCHAR(10),
    registration_date DATE NOT NULL,
    gender ENUM('M', 'F', 'Other'),
    age INT CHECK (age >= 18 AND age <= 120),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Create orders table
CREATE TABLE IF NOT EXISTS orders (
    order_id INT PRIMARY KEY AUTO_INCREMENT,
    user_id INT NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    quantity INT NOT NULL DEFAULT 1,
    unit_price DECIMAL(10,2) NOT NULL,
    total_amount DECIMAL(10,2) NOT NULL,
    order_date TIMESTAMP NOT NULL,
    status ENUM('pending', 'completed', 'cancelled', 'refunded') DEFAULT 'pending',
    payment_method ENUM('credit_card', 'debit_card', 'paypal', 'bank_transfer') NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

-- Create products table
CREATE TABLE IF NOT EXISTS products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    brand VARCHAR(100) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    stock_quantity INT NOT NULL DEFAULT 0,
    in_stock BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Insert sample users data
INSERT INTO users (first_name, last_name, email, phone, address, city, state, zip_code, registration_date, gender, age) VALUES
('John', 'Doe', 'john.doe@example.com', '555-0101', '123 Main St', 'New York', 'NY', '10001', '2023-01-15', 'M', 28),
('Jane', 'Smith', 'jane.smith@example.com', '555-0102', '456 Oak Ave', 'Los Angeles', 'CA', '90210', '2023-02-20', 'F', 32),
('Bob', 'Johnson', 'bob.johnson@example.com', '555-0103', '789 Pine Rd', 'Chicago', 'IL', '60601', '2023-03-10', 'M', 45),
('Alice', 'Brown', 'alice.brown@example.com', '555-0104', '321 Elm St', 'Houston', 'TX', '77001', '2023-04-05', 'F', 29),
('Charlie', 'Wilson', 'charlie.wilson@example.com', '555-0105', '654 Maple Dr', 'Phoenix', 'AZ', '85001', '2023-05-12', 'M', 38);

-- Insert sample products data
INSERT INTO products (product_id, product_name, category, brand, price, stock_quantity, in_stock) VALUES
('P001', 'Wireless Headphones', 'Electronics', 'TechBrand', 99.99, 50, TRUE),
('P002', 'Coffee Maker', 'Appliances', 'HomeBrand', 79.99, 25, TRUE),
('P003', 'Running Shoes', 'Sports', 'SportBrand', 129.99, 100, TRUE),
('P004', 'Laptop Stand', 'Electronics', 'TechBrand', 49.99, 75, TRUE),
('P005', 'Water Bottle', 'Sports', 'SportBrand', 19.99, 200, TRUE);

-- Insert sample orders data
INSERT INTO orders (user_id, product_id, product_name, quantity, unit_price, total_amount, order_date, status, payment_method) VALUES
(1, 'P001', 'Wireless Headphones', 1, 99.99, 99.99, '2023-06-01 10:30:00', 'completed', 'credit_card'),
(2, 'P002', 'Coffee Maker', 1, 79.99, 79.99, '2023-06-02 14:15:00', 'completed', 'paypal'),
(3, 'P003', 'Running Shoes', 2, 129.99, 259.98, '2023-06-03 09:45:00', 'pending', 'debit_card'),
(1, 'P004', 'Laptop Stand', 1, 49.99, 49.99, '2023-06-04 16:20:00', 'completed', 'credit_card'),
(4, 'P005', 'Water Bottle', 3, 19.99, 59.97, '2023-06-05 11:10:00', 'completed', 'bank_transfer');

-- Create indexes for better performance
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_registration_date ON users(registration_date);
CREATE INDEX idx_orders_user_id ON orders(user_id);
CREATE INDEX idx_orders_order_date ON orders(order_date);
CREATE INDEX idx_orders_status ON orders(status);
CREATE INDEX idx_products_category ON products(category);
CREATE INDEX idx_products_brand ON products(brand);

-- Enable binary logging for CDC (required for DMS)
-- This should be set in the RDS parameter group
-- SET GLOBAL binlog_format = 'ROW';
-- SET GLOBAL binlog_row_image = 'FULL';

-- Create a view for CDC demonstration
CREATE VIEW user_orders_summary AS
SELECT 
    u.user_id,
    u.first_name,
    u.last_name,
    u.email,
    COUNT(o.order_id) as total_orders,
    SUM(o.total_amount) as total_spent,
    MAX(o.order_date) as last_order_date
FROM users u
LEFT JOIN orders o ON u.user_id = o.user_id
GROUP BY u.user_id, u.first_name, u.last_name, u.email;

-- Grant necessary permissions for DMS
-- GRANT REPLICATION SLAVE ON *.* TO 'dms_user'@'%';
-- GRANT REPLICATION CLIENT ON *.* TO 'dms_user'@'%';
-- GRANT SELECT ON interview_db.* TO 'dms_user'@'%';
