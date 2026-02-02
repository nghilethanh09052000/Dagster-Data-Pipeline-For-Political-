from datetime import datetime, timedelta


def iterate_each_day_of_year(year: int):
    """
    Create a generator that iterates through all the day of the year.

    Params:
    year: the year you want to iterate of all day of the year

    Returns: Generator of Python datetime
    """

    # Starts on January 1st of the year
    start_date = datetime(year=year, month=1, day=1)
    # Ends at December 31st of the year
    end_date = datetime(year=year, month=12, day=31)

    current_date = start_date

    # Yield the first day of the year
    yield current_date

    # Iterate through all the day in the year
    while current_date < end_date:
        # Add one day to the current_date each iteration
        current_date += timedelta(days=1)

        yield current_date

    # On the last day of the year, yield for the last time
    yield current_date
