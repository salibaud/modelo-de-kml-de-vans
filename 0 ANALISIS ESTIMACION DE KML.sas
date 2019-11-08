
%LET INTERNO= "76331390-5","92475000-6","79506530-K","96582070-1","79707690-2","1701";


OPTIONS COMPRESS=YES;
OPTIONS NOXWAIT;

/********************************************************************************/
/*                               ESTIMACION DE KML                              */
/********************************************************************************/

/*LA ESTIMACION DE KML SE UTILIZA EN CASO DE NO TENER EL KML REGISTRADO DE UN VEHICULO

CARACTERISTICAS POTENCIALES MAS IMPORTANTES:

- APLICACION DEL VEHICULO
- EDAD DEL CLIENTE
- GIRO DE LA EMPRESA (SUPONEMOS QUE UNA VANS SIEMPRE SE USA PARA ALGUNA ACTIVIDAD COMERCIAL


LA NO EXISTENCIA DE UN CLIENTE EN NUESTRAS BASES DE DATOS IMPLICA QUE CONOCEMOS POCO DEL CLIENTE

BUSQUEMOS: 
GIRO SII, FAMILIA, MODELO, SEGMENTO, EDAD CLIENTE, TIPO DE VENTA, EQUIPO_KSA,EQUIPO_ESPECIAL,
TIPO_VEHICULO


/*Muerte de tablas*/
PROC DATASETS nolist LIBRARY=WORK kill;run;

/*carga de librerias*/
%INCLUDE "e:\KAUFMANN\SAS\LIBRERIAS.SAS"/SOURCE2;


/*TUVO CONTRATO ALGUNA VEZ*/
DATA VALIDEZ_CONTRATO; SET KAUFMANN.VALIDEZ_CONTRATO;

IF CLASE_DOCT = "ZFCM" AND MATERIAL NOT = "" AND MATERIAL NOT IN ("CM_FLEETBOARD","FLEETBOARD","FLEETBOARD_HW") THEN CHANGE_DOCT = "X";

RUN;

PROC SQL;
   CREATE TABLE WORK.CHANGE AS 
   SELECT DISTINCT t1.NUM_CONTR, "X" AS CHANGE
      FROM WORK.VALIDEZ_CONTRATO t1
      WHERE t1.CHANGE_DOCT = 'X';
QUIT;

PROC SQL;
   CREATE TABLE WORK.VALIDEZ_CONTRATO AS 
   SELECT t1.*, t2.CHANGE
      FROM WORK.VALIDEZ_CONTRATO t1
           LEFT JOIN WORK.CHANGE t2 ON (t1.NUM_CONTR = t2.NUM_CONTR)
   ORDER BY NUM_CONTR, POS;
QUIT;

DATA VALIDEZ_CONTRATO; 
SET VALIDEZ_CONTRATO; 
IF CHANGE = "X" THEN CLASE_DOCT = "ZTCM"; 
IF CLASE_DOCT NOT = "ZTCM" THEN DELETE; 
RUN;

PROC SORT DATA=VALIDEZ_CONTRATO NODUPKEY; BY CHASSI_VEH2;RUN;


/*VENTA DESDE LEGADOS DE VEHICULOS*/
PROC SQL;
   CREATE TABLE INI_1 AS 
   SELECT t1.RUT, 
          T1.RAZON_SOCIAL,
          t1.NRO_CHASSIS, 
          t1.NRO_VIN, 
          t1.CHASSIS_CORTO, 
          t1.FEC_DOCTO_VTA, 
          t1.PROVEEDOR,
		  T1.PATENTE,
		  T1.SUCURSAL,
		  T3.NOM_CIUDAD AS CIUDAD,
		  T3.NOM_COMUNA AS COMUNA,
		  T1.PROVEEDOR,
		  T1.NETO_VEHICULO_FICHA,
		  T1.TOTAL_DESCUENTO_FICHA,
		  T1.TIPO_FICHA AS FORMA_DE_PAGO,
		  T1.MONEDA_FICHA,
		  T1.TIPO_CAMBIO_FICHA,
		  T1.FAMILIA,
		  T4.TIPO_CONTRATO,
         (CASE WHEN T1.RUT IN (&INTERNO.) THEN 'EXTERNO' ELSE 'INTERNO' END) AS VTA_KAUFMANN
      FROM CLIMABI.BD_ACCOUNT_MANAGER t1 LEFT JOIN UNIDADES.SUCURSAL AS T2 ON (T1.SUCURSAL=T2.NOM_SUC)
	  LEFT JOIN UNIDADES.LOCALIDAD AS T3 ON (T2.COD_LOCALIDAD = T3.COD_LOCALIDAD)
	  LEFT JOIN VALIDEZ_CONTRATO AS T4 ON (T1.NRO_VIN = T4.CHASSI_VEH2 AND T4.CHASSI_VEH2 NE '')
      WHERE  /*AND t1.PROVEEDOR NOT = 'USADOS'*/ year(t1.FEC_DOCTO_VTA) > 2010 
      ORDER BY t1.CHASSIS_CORTO,t1.FEC_DOCTO_VTA,t1.RUT,t1.PROVEEDOR;
QUIT;

/*agrego el giro de la empresa si es que lo es*/

DATA INI_10;
SET INI_1;
RUT_CLI=SUBSTR(RUT,1,INDEX(RUT,"-")-1)*1;
RUN;

PROC SQL;
CREATE TABLE INI_11 AS SELECT
T1.*,
T2.COD_GIRO,
T3.DESC_GIRO,
T3.COD_GIRO_SII
FROM INI_10 AS T1 LEFT JOIN UNIDADES.CLIENTE AS T2 ON (T1.RUT_CLI=t2.RUT_CLI)
LEFT JOIN UNIDADES.GIRO_SII AS T3 ON (T2.COD_GIRO=T3.COD_GIRO);
QUIT;


/*INTENTO COMPLETAR PATENTES CON TODAS LAS BASES DISPONIBLES CONFIABLES*/

PROC SQL;
   CREATE TABLE WORK.QUERY_FOR_VEHICULOS_MOD_CLASIFIC AS 
   SELECT DISTINCT t1.ID_VEH, 
          t1.PATENTE, 
          t1.CHASSIS, 
          t1.DES_MOD1, 
          t1.MODELO, 
          t1.FAMILIA, 
          t1.SEGMENTO
      FROM KAUFMANN.VEHICULOS_MOD_CLASIFICACION t1 WHERE T1.CHASSIS NE '' AND
      LENGTH(COMPRESS(T1.CHASSIS))=17 AND SEGMENTO  IN ("VANS/FURGONES");
QUIT;

/*ESTA TABLA PUEDE TENER DUPLICADOS POR CHASSIS*/
PROC SORT DATA=QUERY_FOR_VEHICULOS_MOD_CLASIFIC NODUPKEY; BY CHASSIS;RUN;

PROC SQL;
CREATE TABLE INI_2 AS SELECT
          t1.RUT, 
          T1.RAZON_SOCIAL,
		  T1.COD_GIRO,
          T1.DESC_GIRO,
          T1.COD_GIRO_SII,
          t1.NRO_CHASSIS, 
          t1.NRO_VIN, 
          t1.CHASSIS_CORTO, 
		  T2.ID_VEH,
          t1.FEC_DOCTO_VTA, 
          t1.PROVEEDOR,  
          (CASE WHEN T1.PATENTE = '' AND T2.PATENTE NE '' THEN T2.PATENTE ELSE T1.PATENTE END) AS PATENTE,
		  T1.SUCURSAL,
		  T1.CIUDAD,
		  T1.COMUNA,
		  T1.PROVEEDOR,
		  T1.NETO_VEHICULO_FICHA,
		  T1.TOTAL_DESCUENTO_FICHA,
		  T1.FORMA_DE_PAGO,
		  T1.MONEDA_FICHA,
		  T1.TIPO_CAMBIO_FICHA,
		  T1.FAMILIA,
		  T1.TIPO_CONTRATO,
		  T1.VTA_KAUFMANN,
		  T2.DES_MOD1,
          T2.MODELO,
          T2.FAMILIA
FROM INI_11 AS T1 
LEFT JOIN QUERY_FOR_VEHICULOS_MOD_CLASIFIC AS T2 ON (T1.NRO_CHASSIS=T2.CHASSIS)
WHERE T2.FAMILIA NE '';
QUIT;

/* ARREGLAMOS LOS PROBLEMAS DE DUPLICIDAD GENERADOS (UN CHASSIS CON DOS VIN´S DISTINTOS)*/

PROC SQL;
CREATE TABLE AAA AS SELECT DISTINCT NRO_CHASSIS,NRO_VIN FROM INI_2;QUIT;

PROC SQL;
   CREATE TABLE WORK.QUERY_FOR_AAA AS 
   SELECT t1.NRO_CHASSIS, 
          /* COUNT_of_NRO_VIN */
            (COUNT(t1.NRO_VIN)) AS COUNT_of_NRO_VIN
      FROM WORK.AAA t1
      GROUP BY t1.NRO_CHASSIS
      ORDER BY COUNT_of_NRO_VIN DESC;
QUIT;

PROC SQL;
CREATE TABLE INI_3 AS SELECT
		  t1.RUT, 
          T1.RAZON_SOCIAL,
		  T1.COD_GIRO,
          T1.DESC_GIRO,
          T1.COD_GIRO_SII,
          t1.NRO_CHASSIS, 
          t1.NRO_VIN, 
          t1.CHASSIS_CORTO, 
		  T1.ID_VEH,
          t1.FEC_DOCTO_VTA, 
          t1.PROVEEDOR,  
          T1.PATENTE,
		  T1.SUCURSAL,
		  T1.CIUDAD,
		  T1.COMUNA,
		  T1.PROVEEDOR,
		  T1.NETO_VEHICULO_FICHA,
		  T1.TOTAL_DESCUENTO_FICHA,
		  T1.FORMA_DE_PAGO,
		  T1.MONEDA_FICHA,
		  T1.TIPO_CAMBIO_FICHA,
		  T1.FAMILIA,
		  T1.TIPO_CONTRATO,
		  T1.VTA_KAUFMANN,
		  T1.DES_MOD1,
          T1.MODELO,
          T1.FAMILIA,
          T2.ZZ_VMS_LV
FROM INI_2 AS T1 LEFT JOIN KAUFMANN.VLCVEHICLE AS T2 ON (T1.ID_VEH=T2.VGUID)
WHERE NRO_CHASSIS NOT IN (SELECT DISTINCT NRO_CHASSIS FROM QUERY_FOR_AAA WHERE COUNT_of_NRO_VIN >1)
ORDER BY T1.ID_VEH DESC,  T1.FEC_DOCTO_VTA DESC;
QUIT;

/*VIGENCIA*/

DATA INI_3_1;
SET INI_3;
IF ID_VEH NE ''  THEN TERMINO_VTA=LAG(FEC_DOCTO_VTA);
IF ID_VEH NE '' THEN  ID_VEH_1=LAG(ID_VEH);
IF ID_VEH_1 NE ID_VEH THEN TERMINO_VTA = .;
FORMAT TERMINO_VTA DDMMYY10.;
DROP ID_VEH_1;
RUN;

PROC SQL;
   CREATE TABLE INI_3_2 AS 
   SELECT 
          t1.RUT, 
          T1.RAZON_SOCIAL,
		  T1.COD_GIRO,
          T1.DESC_GIRO,
          T1.COD_GIRO_SII,
          t1.NRO_CHASSIS, 
          t1.NRO_VIN, 
          t1.CHASSIS_CORTO, 
		  T1.ID_VEH,
          t1.FEC_DOCTO_VTA, 
		  (CASE WHEN t1.TERMINO_VTA=. THEN TODAY() ELSE t1.TERMINO_VTA END) AS TERMINO_VTA FORMAT=DDMMYY10., 
          t1.PROVEEDOR,  
          T1.PATENTE,
		  T1.SUCURSAL,
		  T1.CIUDAD,
		  T1.COMUNA,
		  T1.PROVEEDOR,
		  T1.NETO_VEHICULO_FICHA,
		  T1.TOTAL_DESCUENTO_FICHA,
		  T1.FORMA_DE_PAGO,
		  T1.MONEDA_FICHA,
		  T1.TIPO_CAMBIO_FICHA,
		  T1.FAMILIA,
		  T1.TIPO_CONTRATO,
		  T1.VTA_KAUFMANN,
		  T1.DES_MOD1,
          T1.MODELO,
          T1.FAMILIA,
          T1.ZZ_VMS_LV
      FROM WORK.INI_3_1 t1;
QUIT;


/**************************************************/

/**************************************************/
/********** INFORMACION DEL VEHICULO **************/
/**************************************************/

/**************************************************/




/**************************************************/

/**************************************************/
/********** LLEGADA A SERVICIO       **************/
/**************************************************/

/**************************************************/

PROC SQL;
CREATE TABLE PEDIDO_TIPO_SERVICIO AS SELECT
T1.*
FROM CLIPVCM.PEDIDO_TIPO_SERVICIO AS T1;
QUIT;

PROC SQL;
   CREATE TABLE WORK.QUERY_FOR_PEDIDO_TIPO_SERVICIO AS 
   SELECT t1.PEDIDO, 
          T1.ID_VEH,
          t1.FECHA_APER, 
          t1.KML, 
          t1.DESC_CENTRO, 
          t1.RUT_SOLICITANTE, 
          t1.COBERTURA, 
          (CASE WHEN t1.CATEGORIA='' THEN 'OTRO' ELSE t1.CATEGORIA END) AS CATEGORIA_VISITA, 
          t1.OT_MANT, 
          t1.TIPO_MANT, 
          t1.DESC_MANT, 
          t1.TIPO_PED_CABECERA
      FROM WORK.PEDIDO_TIPO_SERVICIO t1;
QUIT;

PROC SQL;
CREATE TABLE INI_4 AS SELECT
          t1.RUT, 
		  T1.COD_GIRO,
          T1.DESC_GIRO,
          T1.COD_GIRO_SII,
		  t2.RUT_SOLICITANTE,
          T1.RAZON_SOCIAL,
          t1.NRO_CHASSIS, 
          t1.NRO_VIN, 
          t1.CHASSIS_CORTO, 
		  T1.ID_VEH,
          t1.FEC_DOCTO_VTA, 
		  T1.TERMINO_VTA, 
          T1.PATENTE,
		  T1.SUCURSAL,
		  T1.CIUDAD,
		  T1.COMUNA,
		  /*T3.PROVINCIA AS PROVINCIA_CLIENTE,*/
		  T1.PROVEEDOR,
		  T1.NETO_VEHICULO_FICHA,
		  T1.TOTAL_DESCUENTO_FICHA,
		  T1.FORMA_DE_PAGO,
		  T1.MONEDA_FICHA,
		  T1.TIPO_CAMBIO_FICHA,
		  T1.FAMILIA,
		  T1.TIPO_CONTRATO,
		  T1.VTA_KAUFMANN,
		  T1.DES_MOD1,
          T1.MODELO,
          T1.ZZ_VMS_LV,
          t2.PEDIDO, 
          t2.FECHA_APER, 
          t2.KML, 
          t2.DESC_CENTRO, 
          t2.RUT_SOLICITANTE, 
          T2.CATEGORIA_VISITA, 
          t2.OT_MANT, 
          t2.TIPO_MANT, 
          t2.DESC_MANT, 
          t2.TIPO_PED_CABECERA/*,
		  (CASE WHEN T3.COD_SEXO='' THEN '0' ELSE T3.COD_SEXO END) AS SEXO_CLIENTE*/
FROM INI_3_2 AS T1 LEFT JOIN QUERY_FOR_PEDIDO_TIPO_SERVICIO AS T2 ON (T1.ID_VEH=T2.ID_VEH AND T1.FEC_DOCTO_VTA<= t2.FECHA_APER <=T1.TERMINO_VTA
AND T2.ID_VEH NE '')
/*LEFT JOIN CRM.CRM_CLIENTES_CHILE AS T3 ON (T1.RUT=T3.RUT)*/;
QUIT;

DATA CAMBIOS_LEGADO_SERVICIO;
SET INI_4;
IF RUT NE RUT_SOLICITANTE;
IF RUT_SOLICITANTE NE '';
RUT_CLI=SUBSTR(RUT,1,INDEX(RUT_SOLICITANTE,"-")-1)*1;
RUN;

PROC SQL;
   CREATE TABLE ULTIMA_CAMBIOS_LEGADO_SERVICIO AS 
   SELECT t1.RUT_SOLICITANTE, 
          T1.RUT_CLI,
          t1.ID_VEH, 
          /* MAX_of_FECHA_APER */
            (MAX(t1.FECHA_APER)) FORMAT=DDMMYY10. AS FECHA_APER
      FROM WORK.CAMBIOS_LEGADO_SERVICIO t1 WHERE T1.RUT_SOLICITANTE NE '1-9'
      GROUP BY t1.RUT_SOLICITANTE,T1.RUT_CLI,
               t1.ID_VEH;
QUIT;

PROC SQL;
CREATE TABLE CAMBIOS_LEGADO_SERVICIO_1 AS SELECT
T1.RUT_SOLICITANTE,
T1.ID_VEH,
T1.FECHA_APER,
T2.COD_GIRO,
T3.DESC_GIRO,
T3.COD_GIRO_SII
FROM ULTIMA_CAMBIOS_LEGADO_SERVICIO AS T1 LEFT JOIN UNIDADES.CLIENTE AS T2 ON (T1.RUT_CLI=t2.RUT_CLI)
LEFT JOIN UNIDADES.GIRO_SII AS T3 ON (T2.COD_GIRO=T3.COD_GIRO);
QUIT;

PROC SQL;
CREATE TABLE INI_41 AS SELECT
          (CASE WHEN T2.RUT_SOLICITANTE NE '' THEN T2.RUT_SOLICITANTE ELSE t1.RUT END) AS RUT, 
		  (CASE WHEN T2.RUT_SOLICITANTE NE '' THEN T2.COD_GIRO ELSE T1.COD_GIRO END) AS COD_GIRO,
          (CASE WHEN T2.RUT_SOLICITANTE NE '' THEN T2.DESC_GIRO ELSE T1.DESC_GIRO END) AS DESC_GIRO,
          (CASE WHEN T2.RUT_SOLICITANTE NE '' THEN T2.COD_GIRO_SII ELSE T1.COD_GIRO_SII END) AS COD_GIRO_SII,
		  t1.RUT_SOLICITANTE,
          T1.RAZON_SOCIAL,
          t1.NRO_CHASSIS, 
          t1.NRO_VIN, 
          t1.CHASSIS_CORTO, 
		  T1.ID_VEH,
          t1.FEC_DOCTO_VTA, 
		  T1.TERMINO_VTA, 
          T1.PATENTE,
		  T1.SUCURSAL,
		  T1.CIUDAD,
		  T1.COMUNA,
		  T3.PROVINCIA AS PROVINCIA_CLIENTE,
		  T1.PROVEEDOR,
		  T1.NETO_VEHICULO_FICHA,
		  T1.TOTAL_DESCUENTO_FICHA,
		  T1.FORMA_DE_PAGO,
		  T1.MONEDA_FICHA,
		  T1.TIPO_CAMBIO_FICHA,
		  T1.FAMILIA,
		  T1.TIPO_CONTRATO,
		  T1.VTA_KAUFMANN,
		  T1.DES_MOD1,
          T1.MODELO,
          T1.ZZ_VMS_LV,
          t1.PEDIDO, 
          t1.FECHA_APER, 
          t1.KML, 
          t1.DESC_CENTRO, 
          t1.RUT_SOLICITANTE, 
          T1.CATEGORIA_VISITA, 
          t1.OT_MANT, 
          t1.TIPO_MANT, 
          t1.DESC_MANT, 
          t1.TIPO_PED_CABECERA,
		  (CASE WHEN T3.COD_SEXO='' THEN '0' ELSE T3.COD_SEXO END) AS SEXO_CLIENTE
FROM INI_4 AS T1 LEFT JOIN CAMBIOS_LEGADO_SERVICIO_1 AS T2 ON (T1.RUT_SOLICITANTE=T2.RUT_SOLICITANTE AND T1.ID_VEH=T2.ID_VEH AND T1.FECHA_APER<T2.FECHA_APER)
LEFT JOIN CRM.CRM_CLIENTES_CHILE AS T3 ON ((CASE WHEN T2.RUT_SOLICITANTE NE '' THEN T2.RUT_SOLICITANTE ELSE t1.RUT END)=T3.RUT);
QUIT;

PROC SORT DATA=INI_41 NODUPKEY; BY RUT ID_VEH FECHA_APER;RUN;


DATA INI_4_1;
SET INI_41;
IF LENGTH(COMPRESS(CAT(COD_GIRO_SII)))=5 THEN DO;
CODIGO_SII=COMPRESS(CAT(0,SUBSTR(COMPRESS(CAT(COD_GIRO_SII)),1,2)));
END;
IF LENGTH(COMPRESS(CAT(COD_GIRO_SII)))=6 THEN DO;
CODIGO_SII=SUBSTR(COMPRESS(CAT(COD_GIRO_SII)),1,3);
END;
*keep NRO_CHASSIS RUT FAMILIA TIPO_CONTRATO MODELO FEC_DOCTO_VTA FECHA_APER DES_MOD1 SEXO_CLIENTE PROVEEDOR PROVINCIA_CLIENTE NETO_VEHICULO_FICHA MONEDA_FICHA TIPO_CAMBIO_FICHA FORMA_DE_PAGO KML;
RUN;

DATA INI_4_3;
SET INI_4_1;
IF categoria = '' THEN categoria='OTROS';
RUN;

/*************************************/
/*       Inicio Data Cleaning        */
/*************************************/


DATA INI_5;
SET INI_4_3;
/*Generamos el label, KML_POR_MES, el cual se calcula como el 
KML de la medición dividido en el número de días entre la fecha de venta y la fecha de manutención multiplicado por 30.*/
KML_POR_MES = (KML/INTCK('DAY',FEC_DOCTO_VTA,FECHA_APER))*30;
EDAD_ANIOS = INTCK('DAY',FEC_DOCTO_VTA,FECHA_APER)/365;

/*Consideramos solamente las observaciones con 60 o más días a partir de la fecha de venta
(para dejar que los usuarios demuestren cómo se comportan.*/
IF KML NE . THEN DO;
IF INTCK('DAY',FEC_DOCTO_VTA,FECHA_APER) >60 ;
END;
/*La variables MODELO_DESCRIPCION la transformamos en 2 variables nuevas: BLUE_EFFICIENCY
(variable Booleana si el vehículo tiene BLUE EFFICIENCY o no) y MODELO_VEHICULO (variable con el nombre corto del modelo, ej: “A 200”).*/


/*Determinamos la variable MONTO_VENTA_CLP, el cual lleva todos los MONTO_VENTA a pesos chilenos en caso que la moneda haya sido con dólares Kaufmann.*/
MONTO_VENTA=NETO_VEHICULO_FICHA;
IF MONEDA_FICHA NE 'PESO' THEN MONTO_VENTA = NETO_VEHICULO_FICHA*TIPO_CAMBIO_FICHA;

/*Descartamos todas las observaciones de autos usados y aquellas que tenían un contrato de mantención.*/
IF PROVEEDOR = 'USADOS' THEN DELETE;

/*PERSONA JURIDICA O NO*/
TIPO_PERS=0;
IF  (SUBSTR(RUT,1,INDEX(RUT,"-")-1)*1) > 59000000 THEN TIPO_PERS= 1;

RUN;

/*Eliminamos el 5% más pequeño y el 5% mas grande de KML_POR_MES. 
En nuestra muestra los cortos son mayores o iguales a 90 y menores a 8100.*/

PROC MEANS DATA=INI_5 NOPRINT;
WHERE KML NE .;
VAR  KML_POR_MES;
OUTPUT OUT=KML_MES_EXTREMOS (DROP= _TYPE_ _FREQ_)
P5=MINIMO
P95=MAXIMO;
RUN;

PROC SQL NOPRINT; SELECT MINIMO,MAXIMO INTO:MINIMO,:MAXIMO FROM KML_MES_EXTREMOS;QUIT;

DATA CLIPVBM.ML_ESTIMACION_KML_VANS;
SET INI_5;

IF KML_POR_MES NE . THEN DO;

IF &MINIMO.<= KML_POR_MES <= &MAXIMO.;
END;
if MONTO_VENTA ne .;
if FAMILIA IN ('SPRINTER AMBULANCIA','SPRINTER CARGA','SPRINTER ESCOLAR','SPRINTER PASAJEROS','VARIO CARGA','VARIO PASAJEROS','VIANO',
'VITO CARGA',
'VITO ESCOLAR',
'VITO PASAJEROS');
KEEP RUT NRO_CHASSIS EDAD_ANIOS PROVINCIA_CLIENTE PROVEEDOR FAMILIA MODELO TIPO_PERS ANTIGUEDAD KML_POR_MES KML;
RUN;

/*DEJAMOS BASE LIMPIA*/

PROC SQL;
   CREATE TABLE CLIPVBM.ML_ESTIMACION_KML_VANS AS 
   SELECT t1.RUT, 
          t1.NRO_CHASSIS, 
          t1.PROVINCIA_CLIENTE, 
          t1.PROVEEDOR, 
          t1.FAMILIA, 
          t1.MODELO, 
          t1.KML, 
          t1.KML_POR_MES, 
          ROUND(t1.EDAD_ANIOS,.1) AS EDAD_ANIOS, 
          t1.TIPO_PERS
      FROM CLIPVBM.ML_ESTIMACION_KML_VANS t1
      WHERE t1.KML NOT = . AND t1.EDAD_ANIOS NOT IN (.,0);
QUIT;


/************* INCOPORO INFORMACION ADICIONAL DE LEGADOS ************/


PROC SQL;
CREATE TABLE LEGADOS AS select 
v.vhcle, 
v.cod_familia, 
f.desc_familia, 
v.rut_prov, 
p.sigla_prov,
v.ano_pedido, 
v.nro_pedido,
v.cod_marca, 
c.desc_marca,
v.cod_tipo_vehiculo,
t.desc_tipo_vehiculo,
v.nro_vin,
v.nro_chassis, 
v.ano_vehiculo, 
m.desc_modelo_etec, 
m.desc_modelo_fact 
from UNIDADES.v_pedido_veh_nuevo v,
     UNIDADES.modelo_baumuster m,
     UNIDADES.espec_tecnica e,
     UNIDADES.tipo_vehiculo t,
     UNIDADES.proveedor p,
     UNIDADES.familia f,
     UNIDADES.marca c
where f.cod_familia = v.cod_familia and
      c.cod_marca = v.cod_marca and
      v.cod_familia = 0 and
      v.vhcle is not null and
      v.cod_estado_pedido in (3,7) and
      v.cod_tipo_vehiculo not in (15,23,24) and
      p.rut_prov = v.rut_prov and
      e.nro_etec = v.nro_etec and
      t.cod_tipo_vehiculo = v.cod_tipo_vehiculo and
      m.cod_familia = e.cod_familia and
      m.cod_marca = e.cod_marca and
      m.cod_baumuster = e.cod_baumuster and
      m.cod_tipo_vehiculo = e.cod_tipo_vehiculo and
      m.corr_modelo = e.corr_modelo;
QUIT;

PROC SORT DATA=LEGADOS NODUPKEY;BY nro_chassis;RUN;

PROC SQL;
CREATE TABLE CLIPVBM.ML_ESTIMACION_KML_VANS AS SELECT
T2.SIGLA_PROV,
T2.DESC_MARCA,
T2.COD_TIPO_VEHICULO,
T2.DESC_MODELO_FACT,
T1.*
FROM CLIPVBM.ML_ESTIMACION_KML_VANS AS T1 LEFT JOIN LEGADOS AS T2 ON (T1.nro_chassis=T2.NRO_CHASSIS);
QUIT;


/*** EL ANALISIS DESCRIPTIVO INDICA 0 APORTE DE ESTAS VARIABLES ADICIONALES ***/




/******* MODELO DESCRIPTIVO BASE **********/



PROC SORT DATA=CLIPVBM.ML_ESTIMACION_KML_VANS OUT=DATOOOS;BY FAMILIA EDAD_ANIOS;RUN;

PROC MEANS DATA=DATOOOS NOPRINT;
BY FAMILIA EDAD_ANIOS;
VAR KML;
OUTPUT OUT=MODELO_BASICO
MEAN=;
RUN;

PROC SQL;
CREATE TABLE DATOOOS2 AS SELECT
T1.*,
T2.KML AS KML_EST
FROM DATOOOS AS T1 LEFT JOIN MODELO_BASICO AS T2 ON (T1.EDAD_ANIOS=T2.EDAD_ANIOS AND T1.FAMILIA=T2.FAMILIA)
WHERE T1.KML NE .;
QUIT;

data DATOOOS3;
set DATOOOS2;
sum+(KML-KML_EST)**2;
ID=_n_;
mse=SQRT(sum/_n_);
run;

/*ANALISIS BASE 48.000 KML DE ERROR*/




/***************************************************/
/***************************************************/
/*****************   INICIO ANALISIS ***************/
/***************************************************/
/***************************************************/

%LET LIBRERIA=CLIPVBM;
%LET TABLA=ML_ESTIMACION_KML_VANS;
%LET CLASES = PROVINCIA_CLIENTE MODELO FAMILIA;
%LET VAR = EDAD_ANIOS TIPO_PERS;
%let RAND = EDAD_ANIOS;
%LET TARJET = KML;
%LET SUJETO = FAMILIA(MODELO);
%LET MODELO_COMB = PROVINCIA_CLIENTE  FAMILIA  MODELO  EDAD_ANIOS  TIPO_PERS  EDAD_ANIOS*MODELO  EDAD_ANIOS*FAMILIA;
%LET MODELO_COMB2 = PROVINCIA_CLIENTE*EDAD_ANIOS      EDAD_ANIOS  EDAD_ANIOS*TIPO_PERS  EDAD_ANIOS*MODELO  EDAD_ANIOS*FAMILIA;


/*REGRESION LINEAL*/

proc hpreg data=&LIBRERIA..&TABLA. ALPHA=.05;
	CLASS &CLASES.;
	MODEL &TARJET. =  &MODELO_COMB.  /  CLB  STB ; 
	Selection Method = STEPWISE (  SLE=0.05 SLS=0.05 ) ;
	PARTITION FRACTION(TEST=0 VALIDATE=0.5);
	output out=hreg p resid residual r;
run;

data hreg_mse;
set hreg;
sum+(Residual)**2;
ID=_n_;
mse=SQRT(sum/_n_);
run;

data _NULL_;If 0 then set hreg_mse nobs=n; call symputx('nrows',n);stop;run;

DATA MSE1;
SET hreg_mse;
IF ID = &nrows;
TITULO = "REGRESION LINEAL";
KEEP TITULO MSE;
RUN;


/*MODELO GENERAL CON EFECTO PENDIENTE ALEATORIO*/

ods graphics on;
PROC GLIMMIX data=&LIBRERIA..&TABLA. plots =(all) ;
	CLASS &CLASES.;
    MODEL &TARJET. =  &MODELO_COMB2./ s ddfm=bw
    DIST=normal COVB;
    RANDOM  &RAND. /SUBJECT=&SUJETO. TYPE=chol;
output out=hglimmix pred=p resid=r  std=errror residual var ;
RUN;
ODS GRAPHICS OFF;

data hreg_mse;
set hglimmix;
sum+(Resid)**2;
ID=_n_;
mse=SQRT(sum/_n_);
run;

data _NULL_;If 0 then set hreg_mse nobs=n; call symputx('nrows',n);stop;run;

DATA MSE2;
SET hreg_mse;
IF ID = &nrows;
TITULO = "GLMM";
KEEP TITULO MSE;
RUN;



/*ARBOL DE DECISION*/

ODS GRAPHICS ON;
proc hpsplit data=&LIBRERIA..&TABLA. seed=123   ; 
CLASS &CLASES.;
MODEL &TARJET. =  &CLASES. &VAR.;
prune costcomplexity (leaves=40);
	 /* rules file='\\Vm-kfmsasdev\fuentes_externas\CHILE\POST_VENTA\rules.txt';*/
	  partition fraction(validate=0.3 seed=123);
     /* code file='\\Vm-kfmsasdev\fuentes_externas\CHILE\POST_VENTA\hpsplexc.sas';*/
   output out=hpsplout;
run;
ODS GRAPHICS OFF;

data hpsplout_mse1;
set hpsplout;
sum+(KML-P_KML)**2;
ID=_n_;
mse=SQRT(sum/_n_);
run;

data _NULL_;If 0 then set hpsplout_mse1 nobs=n; call symputx('nrows',n);stop;run;

DATA MSE3;
SET hpsplout_mse1;
IF ID = &nrows;
TITULO = "TREE";
KEEP TITULO MSE;
RUN;

/*BOSQUE ALEATORIO*/
ODS GRAPHICS ON;
proc hpforest data=&LIBRERIA..&TABLA. maxtrees=30;
   input &VAR. /level=interval;
   input &CLASES. /level=nominal;
   target &TARJET. /level=interval;
    ods output Baseline=bs ;
run;
ODS GRAPHICS OFF;


data MSe4;
set bs;
mse=SQRT(Value);
TITULO = "BOSQUE";
KEEP TITULO MSE;
run;

/*RED NEURONAL*/
proc dmdb batch data=&LIBRERIA..&TABLA. out=dmdbout dmdbcat=outcat;
var &VAR. &TARJET.;
class &CLASES.;
target &TARJET.;
run;


proc NEURAL data=&LIBRERIA..&TABLA. dmdbcat=outcat
   random=789;
   input &VAR. / level=INT;
   input &CLASES. /level=nominal;
   target &TARJET./level=interval;
   hidden 10;
   prelim;
   train;
   score out=out outfit=fit;
run;

DATA rn_MSE1;
SET OUT;
sum+(KML-P_KML)**2;
ID=_n_;
mse=SQRT(sum/_n_);
RUN;

data _NULL_;If 0 then set rn_MSE1 nobs=n; call symputx('nrows',n);stop;run;

DATA MSE5;
SET rn_MSE1;
IF ID = &nrows;
TITULO = "RN";
KEEP TITULO MSE;
RUN;


/*COMPARACION*/

DATA MSE_FINAL;
SET MSE1 MSE2 MSE3 MSE4 MSE5;
RUN;