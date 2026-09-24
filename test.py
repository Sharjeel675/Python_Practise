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



# class Employee:
#     def __init__(self,name,id):
#         self.name = name
#         self.id = id
#     def ShowDetail(self):
#         print(f"The Name of Employee is:{self.name} and Id is {self.id}" )
# class SubEmployee(Employee):
#     def ShowEmp(self):
#         print(f"This is Junior Employee:{self.name} and Id is {self.id}")


# e = Employee("Muzammil Shah",303)
# f = SubEmployee("Hammas Shahzad Shani",504)
# e.ShowDetail()
# f.ShowEmp()






















# class SeniorEmployee:
#     def __init__(self,name,id,Location):
#         self.name = name
#         self.id = id
#         self.Location = Location
#     def ShowDetail(self):
#         print(f"The Name of Senior Employe is:{self.name} and Batch Id is {self.id} and home location is {self.Location}")
# class JuniorEmployee(SeniorEmployee):
#     def __init__(self,name,id,Location):
#         self.name = name
#         self.id = id
#         self.Location = Location
#     def ShowJuniorDetail(self):
#         print(f"The Junior Employee name is: {self.name},and Batch Id is:{self.id} and Home Location is:{self.name}")
        
# a = SeniorEmployee("Ahmed Ali Solangi",302,"Afghanistan")
# a.ShowDetail()
# b = JuniorEmployee("Hammas Shahzad Shani",403,"America")
# b.ShowJuniorDetail()








# class SeniorEmployee:
#     def __init__(self,name,id,location):
#         self.name = name
#         self.id = id
#         self.location = location
#     def ShowDetailSenior(self):
#         print(f"Senior Employee Name is:{self.name},and Id is:{self.id},and Location is:{self.location}")
# class JuniorEmployee(SeniorEmployee):
#     def ShowJuniorDetail(self):
#         print(f"Junior Employee Name is:{self.name},and Id is:{self.id} and Location is:{self.location}")


# a = SeniorEmployee("Faraz KGTL",987,"Islamabad")
# a.ShowDetailSenior()
# b = JuniorEmployee("Junaid SAPT",786,"Lahore")
# b.ShowJuniorDetail()










########################################OOP Class Instance and Increment############################




class Employee:
    def __init__(self,fname,lname,salary):
        self.fname = fname
        self.lname = lname
        self.salary = salary
    
    def increase_salary(self):
        self.salary = self.salary  * Increment

harry = Employee("Sharjeel","Ahmed Khan",45000)
rohan = Employee("Danyal","Khan",80000)

print(harry.fname)
print(harry.lname)