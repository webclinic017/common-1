import calendar as cal
from calendar import monthrange
from datetime import timedelta
from pandas.tseries.offsets import BMonthBegin, BMonthEnd


from common.utils.relativedelta import relativedelta


def first_day_of_month(day):
    return day.replace(day=1)


def first_business_day_of_month(day, bdays_offset=0):
    offset = BMonthBegin()
    return offset.rollback(day) + relativedelta(bdays=bdays_offset)


def is_weekend(day):
    return day.weekday() in [5, 6]


def last_day_of_month(day):
    return day.replace(day=monthrange(day.year, day.month)[1])


def last_business_day_of_month(day, bdays_offset=0):
    offset = BMonthEnd()
    return offset.rollforward(day) + relativedelta(bdays=bdays_offset)


def next_weekday(day, weekday):
    days_ahead = weekday - day.weekday()
    if days_ahead <= 0:  # Target day already happened this week
        days_ahead += 7
    return day + timedelta(days_ahead)


def days_in_between(calendar, first, second):
    """Number of business days in between 2 dates

    :param calendar: list - Business days calendar
    :param first: date - First Date
    :param second: date - Second Date
    :return: int - Number of days
    """
    idx1 = calendar.get_loc(first)
    idx2 = calendar.get_loc(second)

    return idx2 - idx1


def third_friday(year, month):
    """Returns the date for the 3rd Friday of the month.

    :param year: int - Year
    :param month: int - month
    :return: datetime.date - Date
    """
    calendar = cal.Calendar(firstweekday=cal.SUNDAY)
    month_cal = calendar.monthdatescalendar(year, month)
    fridays = [day for week in month_cal for day in week if day.weekday(
    ) == cal.FRIDAY and day.month == month]

    return fridays[2]


def business_days(calendar, day):
    """Returns an integer indicating the xth number day in the month.

    :param calendar: list - Business days calendar
    :param day: date - Date to be checked
    :return: int - xth day in the month
    """
    first = day.replace(day=1).strftime('%Y-%m-%d')
    days = calendar[calendar >= first]
    return days.get_loc(day) + 1
