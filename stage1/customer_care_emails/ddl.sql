CREATE TABLE IF NOT EXISTS customer_care_emails (
    subject TEXT NOT NULL,
    sender TEXT NOT NULL,
    receiver TEXT NOT NULL,
    "timestamp" TIMESTAMPTZ NOT NULL,
    message_body TEXT NOT NULL,
    thread_id VARCHAR(64) NOT NULL,
    email_types TEXT NOT NULL,
    email_status VARCHAR(16) NOT NULL,
    email_criticality VARCHAR(16) NOT NULL,
    product_types TEXT NOT NULL,
    agent_effectivity VARCHAR(16) NOT NULL,
    agent_efficiency VARCHAR(16) NOT NULL,
    customer_satisfaction DOUBLE PRECISION NOT NULL,

    CONSTRAINT pk_customer_care_emails
        PRIMARY KEY (thread_id, "timestamp", subject),

    CONSTRAINT chk_email_status
        CHECK (email_status IN ('ongoing', 'completed')),

    CONSTRAINT chk_email_criticality
        CHECK (email_criticality IN ('low', 'medium', 'high')),

    CONSTRAINT chk_agent_effectivity
        CHECK (
            agent_effectivity IN (
                'very low', 'low', 'medium', 'high', 'very high'
            )
        ),

    CONSTRAINT chk_agent_efficiency
        CHECK (
            agent_efficiency IN (
                'very low', 'low', 'medium', 'high', 'very high'
            )
        ),

    CONSTRAINT chk_customer_satisfaction
        CHECK (customer_satisfaction BETWEEN -1.0 AND 1.0)
);
