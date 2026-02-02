
CREATE TABLE md_contributions_landing (
  "receiving_committee" text,
  "filing_period" text,
  "contribution_date" text,
  "contributor_dame" text,
  "contributor_address" text,
  "contributor_type" text,
  "contribution_type" text,
  "contribution_amount" text,
  "Employer_name" text,
  "employer_occupation" text,
  "office" text,
  "fundtype" text
);

CREATE TABLE md_expenditures_landing (
    "expenditure_date" text,
    "payee_name" text,
    "address" text,
    "payee_type" text,
	"amount" text,
    "committee_name" text,
    "expense_category" text,
	"expense_purpose" text,
    "expense_toward" text,
    "expense_method" text,
    "vendor" text,
    "fundtype" text,
    "comments" text
)
