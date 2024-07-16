import tNavigator_python_API as tnav

conn = tnav.Connection(path_to_exe='D:/Users/zalozhnovas/AppData/Local/Programs/RFD/tNavigator/24.1/tNavigator-con.exe')
snp_project = conn.open_project(path='D:/tnavtest/Mishaevskoe_01.05.2024.snp', save_on_close=True)
