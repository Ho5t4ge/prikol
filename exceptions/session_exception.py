#  Copyright (c) 2010-2024. LUKOIL-Engineering Limited KogalymNIPINeft Branch Office in Tyumen
#  Данным программным кодом владеет Филиал ООО "ЛУКОЙЛ-Инжиниринг" "КогалымНИПИнефть" в г.Тюмени

class SessionException(Exception):
    def __init__(self):
        super().__init__("Not configured session!")
