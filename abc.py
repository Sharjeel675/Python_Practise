class SeniorEmployee:
    def __init__(self,name,id,location):
        self.name = name
        self.id = id
        self.location = location
    def ShowDetail(self):
        print(f"SeniorEmployee Name is:{self.name} And Id is:{self.id} And location is:{self.location}")

class JuniorEmployee(SeniorEmployee):
    def ShowDetailJunior(self):
        print(f"JuniorEmployee Name is:{self.name} And Id is:{self.id} And location is:{self.location}")
    
class FemaleEmployee(SeniorEmployee):
    def ShowFemaleEmployee(self):
        print(f"SeniorEmployee Name is:{self.name} And Id is:{self.id} And location is:{self.location}")




a = SeniorEmployee("Sharjeel Ahmed Khan",876,"Islamabad")
a.ShowDetail()
b = JuniorEmployee("Muzammil",999,"Lahore")
b.ShowDetailJunior()
c = FemaleEmployee("Laila",777,"kashmir")
c.ShowFemaleEmployee()
print(a.name)