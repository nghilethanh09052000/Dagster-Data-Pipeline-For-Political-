-- Contrib landing table
CREATE TABLE pa_contrib_new_format_landing_table (
    campaignfinanceid text,
    filerid text,
    eyear text,
    submitteddate text,
    cycle text,
    section text,
    contributor text,
    address1 text,
    address2 text,
    city text,
    state text,
    zipcode text,
    occupation text,
    ename text,
    eaddress1 text,
    eaddress2 text,
    ecity text,
    estate text,
    ezipcode text,
    contdate1 text,
    contamt1 text,
    contdate2 text,
    contamt2 text,
    contdate3 text,
    contamt3 text,
    contdesc text
);

-- Debt landing table
CREATE TABLE pa_debt_new_format_landing_table (
    campaignfinanceid text,
    filerid text,
    eyear text,
    submitteddate text,
    cycle text,
    dbtname text,
    address1 text,
    address2 text,
    city text,
    state text,
    zipcode text,
    dbtdate text,
    dbtamt text,
    dbtdesc text
);

-- Expense landing table
CREATE TABLE pa_expense_new_format_landing_table (
    campaignfinanceid text,
    filerid text,
    eyear text,
    submitteddate text,
    cycle text,
    expname text,
    address1 text,
    address2 text,
    city text,
    state text,
    zipcode text,
    expdate text,
    expamt text,
    expdesc text
);

-- Filer landing table
CREATE TABLE pa_filer_new_format_landing_table (
    campaignfinanceid text,
    filerid text,
    eyear text,
    submitteddate text,
    cycle text,
    ammend text,
    terminate text,
    filertype text,
    filername text,
    office text,
    district text,
    party text,
    address1 text,
    address2 text,
    city text,
    state text,
    zipcode text,
    county text,
    phone text,
    beginning text,
    monetary text,
    inkind text
);

-- Receipt landing table
CREATE TABLE pa_receipt_new_format_landing_table (
    campaignfinanceid text,
    filerid text,
    eyear text,
    submitteddate text,
    cycle text,
    recname text,
    address1 text,
    address2 text,
    city text,
    state text,
    zipcode text,
    recdesc text,
    recdate text,
    recamt text
);
