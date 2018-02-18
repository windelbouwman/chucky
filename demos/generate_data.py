
""" Generate some data """

with open('data1.bin', 'wb') as f:
    for _ in range(5):
        for a in range(200):
            f.write(bytes([a, 1, a]))


with open('data2.bin', 'wb') as f:
    f.write(bytes([0xff]))
    for _ in range(10):
        for a in range(200):
            f.write(bytes([a, 1, a]))
