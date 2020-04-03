
--Recurly Bonus calculation : Table DDL


CREATE TABLE [msh].[bonusRecurlyData_Staging](
    location_id VARCHAR(255) NOT NULL,
	business_name VARCHAR(255),
	cancel_date DATETIME,
	current_price DECIMAL(38,20),
	CONSTRAINT loc_id PRIMARY KEY (location_id)
);