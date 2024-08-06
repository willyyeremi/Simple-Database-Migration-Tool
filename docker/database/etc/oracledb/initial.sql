-- Create custom user
CREATE USER custom_user IDENTIFIED BY custom_password;

-- Grant necessary privileges to custom user
GRANT CONNECT, RESOURCE, DBA TO custom_user;