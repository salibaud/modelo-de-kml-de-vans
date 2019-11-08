

/*UNION*/


PROC SQL;
CREATE TABLE SEGMENTO_2_Y_3_ AS SELECT
T1.*,
(CASE WHEN T2._INTO_='0' THEN 2 WHEN T2._INTO_='1' THEN 3 END) AS SEGMENTO,
T2.IP_1 AS PROBABILIDAD_DE_SEGMENTO_3
FROM SEGMENTO_2_Y_3 AS T1 LEFT JOIN QUERY_FOR_PREDLOGREGPREDICTIONSB AS T2 ON
(T1.RUT=T2.RUT AND T1.NRO_CHASSIS=T2.NRO_CHASSIS);
QUIT;

DATA SEGMENTO_1;
SET SEGMENTO_1;
PROBABILIDAD_DE_SEGMENTO_3=0;
RUN;

DATA LISTADO_PROPENSION_VISITA_VANS;
SET SEGMENTO_1 SEGMENTO_2_Y_3_;
RUN;


/*agrego contactos desde crm y descripcion*/

data LISTADO_PROPENSION_VISITA_VANS1;
FORMAT DESCRIPCION $50.;
set LISTADO_PROPENSION_VISITA_VANS;
if SEGMENTO = 1 then DESCRIPCION = "CLIENTE NUNCA A VENIDO A SERVICIO";
IF ANTIGUEDAD_VEHICULO < 12 THEN  DESCRIPCION = "CLIENTE NUEVO";
IF SEGMENTO = 2 THEN DESCRIPCION = "CLIENTE KAUFMANN BAJA PROBABILIDAD DE VENIR";
IF SEGMENTO = 3 THEN DESCRIPCION = "CLIENTE KAUFMANN ALTA PROBABILIDAD DE VENIR";
RUN;

PROC SQL;
CREATE TABLE CLIPVBM.AGF_VANS_3_LISTADO_FINAL AS SELECT
T2.TELEFONO_FIJO,
t2.TELEFONO_MOVIL,
T2.CORREO_ELECTRONICO,
T2.COMUNA,
T2.NOMBRE_DE_PILA,
T2.APELLIDOS,
T2.RAZON_SOCIAL,
/*T3.NOMBRE_PERSONA_CONTACTO,
T3.CORREO_PERSONA_CONTACTO,
T3.CARGO,*/
t1.*
from LISTADO_PROPENSION_VISITA_VANS1 as t1 left join CRM.CRM_CLIENTES_CHILE AS T2 ON (T1.RUT=T2.RUT)
/*LEFT JOIN CRM.CRM_PERSONAS_DE_CONTACTO_CHILE AS T3 ON (T2.ID_CLIENTE=T3.ID_CLIENTE)*/;
QUIT;