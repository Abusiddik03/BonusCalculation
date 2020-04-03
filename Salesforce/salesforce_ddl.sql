
--Salesforce Bonus calculation : Table DDL

CREATE TABLE [msh].[bonusSalesForceData_Staging](
    opportunity_id VARCHAR(255) NOT NULL,
	account_id VARCHAR(255),
	price FLOAT,
	product_name VARCHAR(255),
	opportunity_record_type VARCHAR(255),
	industry  VARCHAR(255),
	rdp_file_number BIGINT,
	location_number FLOAT,
	deal_reversal BIT,
	channel  VARCHAR(255),
	start_DATE DATE,
	close_DATE  DATE,
	CONSTRAINT opp_id PRIMARY KEY (opportunity_id)
);