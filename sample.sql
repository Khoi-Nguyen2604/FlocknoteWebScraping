
CREATE TABLE flocknote_MessagesSent (
    datestamp DATETIME NOT NULL,  
    MessageSent VARCHAR(2083) NOT NULL,
    CollectionDateStamp DATETIME NOT NULL
);
CREATE TABLE flocknote_links (
    datestamp DATETIME NOT NULL,
    MessageSent VARCHAR(2083) NOT NULL,
    link VARCHAR(2083) NOT NULL,
    counts INT DEFAULT 0,
    PRIMARY KEY (link,datestamp)
);
CREATE TABLE flocknote_unsubscribes (
    datestamp DATETIME NOT NULL,
    MessageSent VARCHAR(2083) NOT NULL,
    name VARCHAR(255) NOT NULL,
    PRIMARY KEY (datestamp)
);
CREATE TABLE flocknote_sms (
    datestamp DATETIME NOT NULL,
    MessageSent VARCHAR(2083) NOT NULL,
    name VARCHAR(255),
    phone_number VARCHAR(20) NOT NULL,
    PRIMARY KEY (datestamp,phone_number)
);
CREATE TABLE flocknote_messageAction (
    datestamp DATETIME NOT NULL,
    is_opened BOOLEAN,
    MessageSent VARCHAR NOT NULL, 
    name VARCHAR(255),
    email VARCHAR(255) NOT NULL,
    PRIMARY KEY (datestamp, email)
);
CREATE TABLE flocknote_subscribes ( 
    ID VARCHAR(255), 
    name VARCHAR(255),
    phone_number VARCHAR(20),
    email VARCHAR(255),
    PRIMARY KEY(ID)
);