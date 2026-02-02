{% macro safe_parse_date(date_string) %}
    CASE
        WHEN
            -- Validate basic MM/DD/YYYY format
            {{ date_string }} ~ '^\d{1,2}/\d{1,2}/\d{4}$'
            AND (
                -- 31-day months
                (
                    SPLIT_PART({{ date_string }}, '/', 1)::INT IN (1, 3, 5, 7, 8, 10, 12)
                    AND SPLIT_PART({{ date_string }}, '/', 2)::INT BETWEEN 1 AND 31
                )
                -- 30-day months
                OR (
                    SPLIT_PART({{ date_string }}, '/', 1)::INT IN (4, 6, 9, 11)
                    AND SPLIT_PART({{ date_string }}, '/', 2)::INT BETWEEN 1 AND 30
                )
                -- February
                OR (
                    SPLIT_PART({{ date_string }}, '/', 1)::INT = 2
                    AND (
                        -- Days 1 to 28 always valid
                        SPLIT_PART({{ date_string }}, '/', 2)::INT BETWEEN 1 AND 28
                        -- 29th only valid on leap years
                        OR (
                            SPLIT_PART({{ date_string }}, '/', 2)::INT = 29
                            AND (
                                (
                                    -- Divisible by 400
                                    (SPLIT_PART({{ date_string }}, '/', 3)::INT % 400 = 0)
                                )
                                OR (
                                    -- Divisible by 4 but not 100
                                    (SPLIT_PART({{ date_string }}, '/', 3)::INT % 4 = 0
                                     AND SPLIT_PART({{ date_string }}, '/', 3)::INT % 100 != 0)
                                )
                            )
                        )
                    )
                )
            )
        THEN TO_TIMESTAMP({{ date_string }}, 'MM/DD/YYYY')
        ELSE NULL
    END
{% endmacro %}
