"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd


def ingest_data():

    #
    # Inserte su código aquí
    #

    data = open("clusters_report.txt", 'r')
    
    #Obtencion y limpieza de titulos
    titulo1 = data.readline().split('  ')
    titulo1 = [t for t in titulo1 if t!= '' and t!= ' \n']
    titulo1 = [t.strip() for t in titulo1]
    
    titulo2 = data.readline().split('  ')
    titulo2 = [t for t in titulo2 if t != '']
    titulo2 = [t.strip() for t in titulo2]
    titulo2 = [t.strip('\n') for t in titulo2]
    
    titulos = [titulo1[0], ' '.join([titulo1[1], titulo2[0]]), ' '.join([titulo1[2], titulo2[1]]), titulo1[3]]
    titulos = [t.replace(' ','_') for t in titulos]
    titulos = [t.lower() for t in titulos]
    
    #eliminar los saltos indeseados
    data.readline()
    data.readline()
    
    #obtener resto de datos y limpiarlos
    datos = []
    
    resto = data.read()
    dato1 = resto[:455].split('  ')
    dato1 = [d for d in dato1 if d != '']
    dato1 = [d.strip() for d in dato1]
    dato1 = [d.strip('\n') for d in dato1]
    dato1 = dato1[:3] + [' '.join(dato1[3:])]
    dato1[0] = int(dato1[0])
    dato1[1] = int(dato1[1])
    dato1[2] = float(dato1[2].strip(' %').replace(',','.'))
    dato1[3] = dato1[3].strip('.')
    datos.append(dato1)
    
    
    dato2 = resto[455:977].split('  ')
    dato2 = [d for d in dato2 if d != '']
    dato2 = [d.strip() for d in dato2]
    dato2 = [d.strip('\n') for d in dato2]
    dato2 = dato2[:3] + [' '.join(dato2[3:])]
    dato2[0] = int(dato2[0])
    dato2[1] = int(dato2[1])
    dato2[2] = float(dato2[2].strip(' %').replace(',','.'))
    dato2[3] = dato2[3].strip('. ')
    datos.append(dato2)
    
    
    dato3 = resto[977:1305].split('  ')
    dato3 = [d for d in dato3 if d != '']
    dato3 = [d.strip() for d in dato3]
    dato3 = [d.strip('\n') for d in dato3]
    dato3 = dato3[:3] + [' '.join(dato3[3:])]
    dato3[0] = int(dato3[0])
    dato3[1] = int(dato3[1])
    dato3[2] = float(dato3[2].strip(' %').replace(',','.'))
    dato3[3] = dato3[3].strip('. ')
    datos.append(dato3)
    
    
    dato4 = resto[1305:1745].split('  ')
    dato4 = [d for d in dato4 if d != '']
    dato4 = [d.strip() for d in dato4]
    dato4 = [d.strip('\n') for d in dato4]
    dato4 = dato4[:3] + [' '.join(dato4[3:])]
    dato4[0] = int(dato4[0])
    dato4[1] = int(dato4[1])
    dato4[2] = float(dato4[2].strip(' %').replace(',','.'))
    dato4[3] = dato4[3].strip('. ')
    datos.append(dato4)
    
    
    dato5 = resto[1745:2165].split('  ')
    dato5 = [d for d in dato5 if d != '']
    dato5 = [d.strip() for d in dato5]
    dato5 = [d.strip('\n') for d in dato5]
    dato5 = dato5[:3] + [' '.join(dato5[3:])]
    dato5[0] = int(dato5[0])
    dato5[1] = int(dato5[1])
    dato5[2] = float(dato5[2].strip(' %').replace(',','.'))
    dato5[3] = dato5[3].strip('. ')
    datos.append(dato5)
    
    
    dato6 = resto[2165:2635].split('  ')
    dato6 = [d for d in dato6 if d != '']
    dato6 = [d.strip() for d in dato6]
    dato6 = [d.strip('\n') for d in dato6]
    dato6 = dato6[:3] + [' '.join(dato6[3:])]
    dato6[0] = int(dato6[0])
    dato6[1] = int(dato6[1])
    dato6[2] = float(dato6[2].strip(' %').replace(',','.'))
    dato6[3] = dato6[3].strip('. ')
    datos.append(dato6)
    
    
    dato7 = resto[2635:3100].split('  ')
    dato7 = [d for d in dato7 if d != '']
    dato7 = [d.strip() for d in dato7]
    dato7 = [d.strip('\n') for d in dato7]
    dato7 = dato7[1:4] + [' '.join(dato7[3:])]
    dato7[0] = int(dato7[0])
    dato7[1] = int(dato7[1])
    dato7[2] = float(dato7[2].strip(' %').replace(',','.'))
    dato7[3] = dato7[3].strip('. ')
    dato7[3] = dato7[3][6:]
    datos.append(dato7)
    
    
    dato8 = resto[3105:3550].split('  ')
    dato8 = [d for d in dato8 if d != '']
    dato8 = [d.strip() for d in dato8]
    dato8 = [d.strip('\n') for d in dato8]
    dato8 = dato8[:3] + [' '.join(dato8[3:])]
    dato8[0] = int(dato8[0])
    dato8[1] = int(dato8[1])
    dato8[2] = float(dato8[2].strip(' %').replace(',','.'))
    dato8[3] = dato8[3].strip('. ')
    datos.append(dato8)
    
    
    dato9 = resto[3550:3990].split('  ')
    dato9 = [d for d in dato9 if d != '']
    dato9 = [d.strip() for d in dato9]
    dato9 = [d.strip('\n') for d in dato9]
    dato9 = dato9[1:4] + [' '.join(dato9[3:])]
    dato9[0] = int(dato9[0])
    dato9[1] = int(dato9[1])
    dato9[2] = float(dato9[2].strip(' %').replace(',','.'))
    dato9[3] = dato9[3].strip('. ')
    dato9[3] = dato9[3][6:]
    datos.append(dato9)
    
    
    dato10 = resto[3990:4410].split('  ')
    dato10 = [d for d in dato10 if d != '']
    dato10 = [d.strip() for d in dato10]
    dato10 = [d.strip('\n') for d in dato10]
    dato10 = dato10[:3] + [' '.join(dato10[3:])]
    dato10[0] = int(dato10[0])
    dato10[1] = int(dato10[1])
    dato10[2] = float(dato10[2].strip(' %').replace(',','.'))
    dato10[3] = dato10[3].strip('. ')
    datos.append(dato10)
    
    
    dato11 = resto[4410:4640].split('  ')
    dato11 = [d for d in dato11 if d != '']
    dato11 = [d.strip() for d in dato11]
    dato11 = [d.strip('\n') for d in dato11]
    dato11 = dato11[1:4] + [' '.join(dato11[3:])]
    dato11[0] = int(dato11[0])
    dato11[1] = int(dato11[1])
    dato11[2] = float(dato11[2].strip(' %').replace(',','.'))
    dato11[3] = dato11[3].strip('. ')
    dato11[3] = dato11[3][6:]
    datos.append(dato11)
    
    
    dato12 = resto[4640:5215].split('  ')
    dato12 = [d for d in dato12 if d != '']
    dato12 = [d.strip() for d in dato12]
    dato12 = [d.strip('\n') for d in dato12]
    dato12 = dato12[1:4] + [' '.join(dato12[3:])]
    dato12[0] = int(dato12[0])
    dato12[1] = int(dato12[1])
    dato12[2] = float(dato12[2].strip(' %').replace(',','.'))
    dato12[3] = dato12[3].strip('. ')
    dato12[3] = dato12[3][6:]
    datos.append(dato12)
    
    
    dato13 = resto[5215:].split('  ')
    dato13 = [d for d in dato13 if d != '']
    dato13 = [d.strip() for d in dato13]
    dato13 = [d.strip('\n') for d in dato13]
    dato13 = dato13[:3] + [' '.join(dato13[3:])]
    dato13[0] = int(dato13[0])
    dato13[1] = int(dato13[1])
    dato13[2] = float(dato13[2].strip(' %').replace(',','.'))
    dato13[3] = dato13[3].strip('. ')
    datos.append(dato13)
    
    
    #Organizar y crear dataframe
    indices = [d[0] for d in datos]
    cant_pal_claves = [d[1] for d in datos]
    porcentajes = [d[2] for d in datos]
    palabras_claves = [d[3] for d in datos]
    
    df = pd.DataFrame(
        data= {
            titulos[0] : cant_pal_claves,
            titulos[1] : porcentajes,
            titulos[2] : palabras_claves,
        },
        index = indices
    )
    
    return df
