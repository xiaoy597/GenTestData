import numpy as np
import sys
import datetime

if len(sys.argv) < 2:
    print "Need to specify the number of records for generating data."
    exit(1)

number_of_records = sys.argv[1]

norm_rand_values = np.random.normal(0.5, 0.01, int(number_of_records))

# print norm_rand_values[:100]

uni_rand_values = np.random.random(int(number_of_records))

# print uni_rand_values[:100]

code_def_list = ["%03d" % i for i in range(200)]

# print code_def_list

code_list = [code_def_list[int(abs(i) * 200) % 200] for i in iter(norm_rand_values)]
# print code_list[:100]

start_date = datetime.datetime.strptime('2005-04-20 00:00:00', '%Y-%m-%d %H:%M:%S')

date_list = []
for i in iter(norm_rand_values):
    date_list.append((start_date + datetime.timedelta(days=7) * int(i * 1000)).strftime('%Y-%m-%d'))

# print date_list[:100]

ts_list = []
for i in iter(uni_rand_values):
    ts_list.append(
        (start_date + datetime.timedelta(days=7) * int(i * 1000) + datetime.timedelta(seconds=7) * int(i * 100000))
            .strftime('%Y-%m-%d %H:%M:%S'))

# print ts_list[:100]

char_list = [str(i)[:10] for i in iter(norm_rand_values)]

# print char_list[:100]

float_list = [i * 1000 for i in iter(uni_rand_values)]

# print float_list[:100]

double_list = [(i * 1000) ** 3 for i in iter(float_list)]

# print double_list[:100]

varchar_list = [str(i) for i in map(lambda (a, b): a * b, zip(double_list, float_list))]

# print varchar_list[:100]

rowNum = 0
while rowNum < int(number_of_records):
    print "%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s" % \
          (rowNum
           , '\N' if rowNum % 17 == 0 else code_list[rowNum]
           , char_list[rowNum]
           , varchar_list[rowNum]
           , '\N' if rowNum % 11 == 0 else date_list[rowNum]
           , str(long(uni_rand_values[rowNum] * 1000000))
           , str(float_list[rowNum])
           , str(double_list[rowNum])
           , "%.4f" % (norm_rand_values[rowNum] * 10 ** 9)
           , ts_list[rowNum]
           , "true" if uni_rand_values[rowNum] > 0.5 else "false")
    rowNum += 1
