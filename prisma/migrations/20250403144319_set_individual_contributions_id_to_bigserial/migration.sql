-- Create bigserial sequence for individual contributions id
CREATE SEQUENCE master_individual_contributions_id_seq AS bigint;

-- Set sequence to be used by id column
ALTER TABLE master_individual_contributions ALTER COLUMN id SET DEFAULT nextval('master_individual_contributions_id_seq');
ALTER SEQUENCE master_individual_contributions_id_seq OWNED BY master_individual_contributions.id;
