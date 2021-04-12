import holder_pb2


class ReadCardHolder:
    def __init__(self):
        self.card_holder = holder_pb2.CardHolder()

    def ListPeople(self):
        print("Name:", self.card_holder.name)
        print("Job:", self.card_holder.job)
        print("Phone Number:", self.card_holder.phone_number)
        print("Address:", self.card_holder.address)
        print("Card Number:", self.card_holder.card_number)
        print("Card Provider:", self.card_holder.card_provider)


if __name__ == "__main__":
    read_card_holder = ReadCardHolder()
    print('============')
    f = open("records.bin", "rb")
    a = f.read()
    b = a.split(b';')[:-1]
    for c in b:
        read_card_holder.card_holder.ParseFromString(c)
        read_card_holder.ListPeople()
