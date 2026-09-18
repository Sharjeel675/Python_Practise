# class Person:
#     def __init__(self):
#         print("Hey i am a Data Engineer")
# a = Person()



# class Person:
#     def __init__(self,name,occupation):
#         self.name = name
#         self.occupation = occupation
# a = Person("Sharjeel Ahmed KHan" ,"Data Engineer")
# print(a.name,a.occupation)




# class Person:
#     def __init__(self,name,occupation,age,Location):
#         self.name = name
#         self.occupation = occupation
#         self.age = age
#         self.Location = Location
# a = Person("Sharjeel Ahmed Khan", "Data Engineer",25,"Orangi Town Karachi")
# print(a.name,a.age,a.occupation,a.Location)








########################################INHERITANCE###############################################################



class Employee:
    def __init__(self,name,id):
        self.name = name
        self.id = id
    def ShowDetail(self):
        print(f"The Name of Employee is:{self.name} and Id is {self.id}" )
class SubEmployee(Employee):
    def ShowEmp(self):
        print(f"This is Junior Employee:{self.name} and Id is {self.id}")


e = Employee("Muzammil Shah",303)
f = SubEmployee("Hammas Shahzad Shani",504)
e.ShowDetail()
f.ShowEmp()