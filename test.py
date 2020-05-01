for i in range(6):
    globals()['num_%s' % i] = i ** 2
    print(globals()['num_%s' % i])
# print(num_1)  # 1
# print(num_2)  # 4
# print(num_3)  # 9
# print(num_4)  # 16
# print(num_5)