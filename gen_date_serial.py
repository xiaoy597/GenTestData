from datetime import timedelta
from datetime import date

day = timedelta(days=1)

current_date = date(2015, 1, 1)

while current_date < date(2018, 1, 1):
    print str(current_date) + '||||||||||' + '1' + '|||||||||||||'
    current_date = current_date + day



