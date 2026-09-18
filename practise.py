

# class Person:
#     name = 'Sharjeel Ahmed Khan'
#     occupation = 'Software Developer'
#     networth = 10
#     def info(self):
#         print(f"{self.name} is a {self.occupation}")

# a = Person()
# a.name = "Hammas"
# a.occupation = "Farig"
# print(a.name, a.occupation)


#############################################CLASS#####################################


class Person:
    name = 'Sharjeel Ahmed Khan'
    occupation = 'Software Developer'
    networth = 10

    def info(self):
        print(f"{self.name} is a {self.occupation}")

a = Person()

a.name = "Hammas"
a.occupation = "Farig"

print(a.name, a.occupation)
a.info()





#############################################CONSTRUCTOR#####################################




class Person:
    def __init__(self):
        print("Hey I am a Data Engineer")

a = Person()