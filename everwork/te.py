import multiprocessing
import threading
import time


def f():
    return 1


def main():
    l = []

    ss = 0
    for _ in range(1000):
        s = time.perf_counter()

        t = threading.Thread(target=f)
        # t = multiprocessing.Process(target=f)
        t.start()

        ss += time.perf_counter() - s

        l.append(t)

    for x in l:
        x.join()

    print(ss * 1000)


if __name__ == '__main__':
    main()
