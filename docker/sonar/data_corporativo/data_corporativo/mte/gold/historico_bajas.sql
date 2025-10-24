-- Databricks notebook source

select
    u.IdUsuario,
    b.DNI,
    u.EmailCorporativo,
    u.FechaAlta,
    u.FechaBaja,
    u.`AñosAntiguedad` as antiguedad,
    -- dim_Edad_alta.categoria_edad as categoria_Edad_alta,
    -- dim_Edad_baja.categoria_edad as categoria_Edad_baja,
    -- jornada, -- cargaran dentro de poco en mte
    -- tipo_contrato, -- cargaran dentro de poco en mte
    u.ModoTrabajo ,
 
    u.ProvieneDe,
    u.Recruiter,
    -- DIMENSION_TIPO_PERFIL.des_tipo_perfil as tipo_perfil, -- ????
    -- DIMENSION_NIVELES.des_nivel_usuario as nivel_usuario,
    gn.GrupoNivel, 
    np.NivelProfesional, 
    cp.CarreraProfesional,
   
    e.Empresa,

    -- DIMENSION_HMA.des_hma as hma,
    -- DIMENSION_HBU.des_hbu as hbu,
    -- DIMENSION_SUBHBU.des_subhbu as subhbu,
    -- DIMENSION_EQUIPO_HBU.des_equipo_hbu as equipo_hbu,
    -- DIMENSION_EQUIPO_multiarea.id_equipo as equipo_multiarea,
    -- DIMENSION_HGZ.des_hgz as hgz,
    -- DIMENSION_HGR.des_hgr as hgr,
    -- DIMENSION_SUBHGR.des_subhgr as subhgr,
    hma.hMA, 
    hbu.hBU, 
    subhbu.subhBU,
    eth.EquipoHBU,
    etm.EquipoMultitarea,
    hgz.hGZ, 
    hgr.hGR, 
    subhgr.subhGR,


    -- DIMENSION_TERRITORIOS.des_territorio as territorio, -- ????
    -- DIMENSION_UBICACIONES.des_ubicacion_usuario as ubicacion,  -- ???? maybe desde hEcosystem y cosas asi?
    -- DIMENSION_CENTROS_TRABAJO.des_Centro_trabajo as centro_trabajo, -- ???
 
    b.MotivoBaja, -- reviasr si motivo baja/origen baja estan girados
    -- b.grupomotivo,
    -- b.razonesbaja,
    -- b.entrevistasalida,
    b.estadopeticion,
    b.observaciones,
    -- b.fecharespuesta,
    -- b.mantenercontacto,
    -- b.sugerencia,
    -- b.fechaaprobacionrrhh,
    b.ComentariosAprobadorRRHH,
    b.AprobadorRRHH,
    b.TipoBaja
 
  from people.gold_mte.dim_baja as b
  left join people.gold_mte.fact_usuarios u on u.IdUsuario = b.IdUsuario
  -- left join [MTE].[DIMENSION_ANTIGUEDAD] on u.id_antiguedad = DIMENSION_ANTIGUEDAD.id_antiguedad
  -- left join [MTE].[DIMENSION_CANAL] on u.id_canal_recruiting = DIMENSION_CANAL.id_canal
  -- left join [MTE].[DIMENSION_CENTROS_TRABAJO] on u.ID_CENTRO_TRABAJO = DIMENSION_CENTROS_TRABAJO.ID_CENTRO_TRABAJO
  -- left join [MTE].[DIMENSION_EDAD] dim_Edad_alta on u.id_edad_alta = dim_Edad_alta.id_edad
  -- left join [MTE].[DIMENSION_EDAD] dim_Edad_baja on u.id_edad_baja = dim_Edad_baja.id_edad
  left join people.gold_mte.dim_empresas e on u.IdEmpresa = e.IdEmpresa
  left join people.gold_mte.dim_equipostrabajohbu eth on u.IdEquipoTrabajohBU = eth.IdEquipoHBU
  left join people.gold_mte.dim_equipostrabajomultiarea etm on u.IdEquipoTrabajoMultiarea = etm.IdEquipoMultitarea 
  left join people.gold_mte.dim_hbu hbu on u.IdhBU = hbu.IdhBU
  left join people.gold_mte.dim_hgr hgr on u.IdhGR = hgr.IdhGR
  left join people.gold_mte.dim_hgz hgz on u.IdhGZ = hgz.IdhGZ
  left join people.gold_mte.dim_hma hma on u.IdhMA = hma.IdhMA
  -- left join [MTE].[DIMENSION_MODO_TRABAJO] on u.id_modo = DIMENSION_MODO_TRABAJO.id_modo

  -- left join [MTE].[DIMENSION_NIVELES] on u.id_nivelactingas = DIMENSION_NIVELES.id_nivel_usuario
  left join people.gold_mte.dim_nivelprofesional np on u.IdNivelProfesional = np.IdNivelProfesional
  left join people.gold_mte.dim_gruponivel gn on u.IdGrupoNivel = gn.IdGrupoNivel
  left join people.gold_mte.dim_carreraprofesional cp on u.IdCarreraProfesional = cp.IdCarreraProfesional

  -- left join [MTE].[DIMENSION_RECRUITER] on u.id_RECRUITER = DIMENSION_RECRUITER.id_RECRUITER
  left join people.gold_mte.dim_subhbu subhbu on u.IdSubhBU = subhbu.IdSubhBU
  left join people.gold_mte.dim_subhgr subhgr on u.IdSubhGR = subhgr.IdSubhGR
  -- left join [MTE].[DIMENSION_TERRITORIOS] on u.id_territorio = DIMENSION_TERRITORIOS.id_territorio
  -- left join [MTE].[DIMENSION_TIPO_PERFIL] on u.id_tipo_perfil = DIMENSION_TIPO_PERFIL.id_tipo_perfil
  -- left join [MTE].[DIMENSION_UBICACIONES] on u.id_ubicacion = DIMENSION_UBICACIONES.id_ubicacion_usuario
 
  where u.IdUsuario is not null


