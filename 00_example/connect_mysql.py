import pymysql

if __name__ == '__main__':
    conn = pymysql.connect(host='192.168.137.73', port=3306, user='GraduationDdesign', password='244773083',
                           database='datax',
                           charset='utf8')
