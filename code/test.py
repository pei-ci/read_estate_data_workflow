class Cars:
    # 建構式
    def __init__(self, color, seat):
        self.color = color  # 顏色屬性
        self.seat = seat  # 座位屬性
    # 方法(Method)

    def drive(self):
        print(f"My car is {self.color} and has {self.seat} seats.")


mazda = Cars("blue", 4)
mazda.drive()  # 執行結果：My car is blue and has 4 seats.
