#!/usr/bin/env python
# coding: utf-8

# In[ ]:

# Autores/as: Nahir Saddi y Guido Canevello

# Importamos las librerías necesarias, nos conectamos al HDFS para recuperar el archivo almacenado con anterioridad
# y realizamos una operación de Select para observar la columna ISO y buscar tuplas inconsistentes.
# REEMPLAZAR LAS DIRECCIONES PARA CADA ACCESO A HADOOP
import findspark
import pyspark
findspark.init()
from pyspark.sql import SparkSession
from operator import add
from optimus import Optimus 
import matplotlib.pyplot as plt
import numpy

sparkSession = SparkSession.builder.appName("ejercicio-informe").getOrCreate()
op = Optimus(sparkSession)
df = sparkSession.read.csv('hdfs://*servidorHDFS*/*direccion del archivo*/fuente3.1.csv', header=True, multiLine=True)

df.cols.select(["ISO"]).rdd.map(lambda x: (x,1)).reduceByKey(add).collect()


# In[ ]:


# Luego de observar los resultados de la consulta, se corrigió manualmente aquellas tuplas inconsistentes 
# dentro del .csv original y se publicó la fuente mejorada nuevamente en el sistema HDFS mediante el programa Java.

df = sparkSession.read.csv('hdfs://*servidorHDFS*/*direccion del archivo*/fuente3.1_mejorada.csv', header=True, multiLine=True)

# Eliminamos las columnas, "ID, PCODE, LINK"
df=df.drop("ID", "PCODE", "LINK")

# Eliminamos filas duplicadas
df=df.dropDuplicates()

# Reemplazar valores vacios de la columna COMMENTS por "sin comentarios"
df=df.fillna({'COMMENTS': 'Sin comentarios'})

# Cargamos en HDFS el nuevo archivo de csv con la información procesada 
df.write.csv("hdfs://*servidorHDFS*/*direccion del archivo*/fuente3.1_procesada", header=True)


# In[ ]:


# Al finalizar la Limpieza de Datos, obtenemos la fuente ya procesada y generamos el gráfico de torta 
# de Cantidad de Medidas por Region.
df = sparkSession.read.csv('hdfs://*servidorHDFS*/*direccion del archivo*/fuente3.1_procesada3/part-*nombre*.csv', header=True, multiLine=True)

# Etiquetas de las distintas regiones
labels = df.cols.select(["REGION"]).distinct().rdd.map(lambda x: ((x[0]))).collect()
# Cantidad de medidas en cada region
sizes = df.cols.select(["REGION"]).rdd.map(lambda x: ((x[0]), 1)).reduceByKey(add).map(lambda x: (x[1])).collect()

# Configuracion del grafico
fig1, ax1 = plt.subplots()
ax1.pie(sizes, labels=labels, autopct='%1.1f%%', shadow=True, startangle=90)
ax1.axis('equal')
ax1.set_title("Proporción de Medidas")
plt.show()


# In[ ]:


# Luego generamos el gráfico de torta de Cantidad de Poblacion por Region.
# Etiquetas de las distintas regiones
labels = df.cols.select(["REGION"]).distinct().rdd.map(lambda x: ((x[0]))).collect()
# Poblacion en Millones de personas por region, extraidos de Wikipedia
sizes = [4463, 741, 1216, 1002, 41, 320]

# Configuracion del grafico
fig1, ax1 = plt.subplots()
ax1.pie(sizes, labels=labels, autopct='%1.1f%%', shadow=True, startangle=90)
ax1.axis('equal')
ax1.set_title("Proporción de Población")
plt.show()


# In[ ]:


# Finalmente generamos el gráfico de torta de Cantidad de Casos por Region.
# Etiquetas de las distintas regiones
labels = df.cols.select(["REGION"]).distinct().rdd.map(lambda x: ((x[0]))).collect()
# Cantidad de casos de cada region, extraido de worldometers.info
sizes = [3620418, 2898496, 961388, 5651507, 19709, 829511]

# Configuracion del grafico
fig1, ax1 = plt.subplots()
ax1.pie(sizes, labels=labels, autopct='%1.1f%%', shadow=True, startangle=90)
ax1.axis('equal')
ax1.set_title("Proporción de Casos")
plt.show()


# In[ ]:


# Para expandir el analisis, generamos el grafico de barras observando las medidas en cada region subdivididas por Categoria
# Etiquetas de las distintas regiones
labels = df.cols.select(["REGION"]).distinct().rdd.map(lambda x: ((x[0]))).collect()
# Categoria por Region
datos = df.cols.select(["CATEGORY", "REGION"])

# Para observar los datos extraidos de la fuente ejecutar la consulta
# datos.rows.select(df["CATEGORY"]=="Public health measures").rdd.map(lambda x: ((x[0], x[1]), 1)).reduceByKey(add).collect()
ph_means = numpy.array([1002, 1630, 837, 936, 338, 203])
# datos.rows.select(df["CATEGORY"]=="Governance and socio-economic measures").rdd.map(lambda x: ((x[0], x[1]), 1)).reduceByKey(add).collect()
gov_means = numpy.array([572, 1072, 499, 636, 248, 76])
# datos.rows.select(df["CATEGORY"]=="Social distancing").rdd.map(lambda x: ((x[0], x[1]), 1)).reduceByKey(add).collect()
dist_means = numpy.array([472, 1261, 783, 507, 238, 194])
# datos.rows.select(df["CATEGORY"]=="Movement restrictions").rdd.map(lambda x: ((x[0], x[1]), 1)).reduceByKey(add).collect()
movm_means = numpy.array([723, 816, 696, 655, 253, 356])
# datos.rows.select(df["CATEGORY"]=="Lockdown").rdd.map(lambda x: ((x[0], x[1]), 1)).reduceByKey(add).collect()
lock_means = numpy.array([143, 110, 181, 188, 29, 71])
# datos.rows.select(df["CATEGORY"]=="Humanitarian exemption").rdd.map(lambda x: ((x[0], x[1]), 1)).reduceByKey(add).collect()
humn_means = numpy.array([0, 1, 3, 7, 0, 1])

width = 0.35

fig, ax = plt.subplots()

# Configuracion del grafico
ax.bar(labels, ph_means, width, label='C1') # Public health measures
a = ph_means
ax.bar(labels, gov_means, width, bottom=a, label='C2') # Governance and socio-economic measures
a = a + gov_means
ax.bar(labels, dist_means, width, bottom=a, label='C3') # Social distancing
a = a + dist_means
ax.bar(labels, movm_means, width, bottom=a, label='C4') # Movement restrictions
a = a + movm_means
ax.bar(labels, lock_means, width, bottom=a, label='C5') # Lockdown
a = a + lock_means
ax.bar(labels, humn_means, width, bottom=a, label='C6') # Humanitarian exemption

ax.set_ylabel('Cantidad de medidas tomadas')
ax.set_title('Medidas tomadas por Región y Categoría')
ax.legend()

plt.show()

