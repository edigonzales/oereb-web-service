package ch.ehi.oereb.webservice;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;

import javax.xml.bind.JAXBElement;
import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.postgresql.util.Base64;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.InputStreamResource;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Controller;
import org.springframework.util.FileSystemUtils;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.io.ByteOrderValues;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;

import ch.ehi.oereb.schemas.oereb._2_0.extract.GetCapabilitiesResponse;
import ch.ehi.oereb.schemas.oereb._2_0.extract.GetCapabilitiesResponseType;
import ch.ehi.oereb.schemas.oereb._2_0.extract.GetEGRIDResponse;
import ch.ehi.oereb.schemas.oereb._2_0.extract.GetEGRIDResponseType;
import ch.ehi.oereb.schemas.oereb._2_0.extract.GetExtractByIdResponse;
import ch.ehi.oereb.schemas.oereb._2_0.extract.GetExtractByIdResponseType;
import ch.ehi.oereb.schemas.oereb._2_0.extractdata.CantonCodeType;
import ch.ehi.oereb.schemas.oereb._2_0.extractdata.DocumentType;
import ch.ehi.oereb.schemas.oereb._2_0.extractdata.DocumentTypeCodeType;
import ch.ehi.oereb.schemas.oereb._2_0.extractdata.DocumentTypeType;
import ch.ehi.oereb.schemas.oereb._2_0.extractdata.DisclaimerType;
import ch.ehi.oereb.schemas.oereb._2_0.extractdata.Extract;
import ch.ehi.oereb.schemas.oereb._2_0.extractdata.ExtractType;
import ch.ehi.oereb.schemas.oereb._2_0.extractdata.GeometryType;
import ch.ehi.oereb.schemas.oereb._2_0.extractdata.GlossaryType;
import ch.ehi.oereb.schemas.oereb._2_0.extractdata.LanguageCodeType;
import ch.ehi.oereb.schemas.oereb._2_0.extractdata.LawstatusCodeType;
import ch.ehi.oereb.schemas.oereb._2_0.extractdata.LawstatusType;
import ch.ehi.oereb.schemas.oereb._2_0.extractdata.LegendEntryType;
import ch.ehi.oereb.schemas.oereb._2_0.extractdata.LocalisedBlobType;
import ch.ehi.oereb.schemas.oereb._2_0.extractdata.LocalisedMTextType;
import ch.ehi.oereb.schemas.oereb._2_0.extractdata.LocalisedTextType;
import ch.ehi.oereb.schemas.oereb._2_0.extractdata.LocalisedUriType;
import ch.ehi.oereb.schemas.oereb._2_0.extractdata.MapType;
import ch.ehi.oereb.schemas.oereb._2_0.extractdata.MultilingualBlobType;
import ch.ehi.oereb.schemas.oereb._2_0.extractdata.MultilingualMTextType;
import ch.ehi.oereb.schemas.oereb._2_0.extractdata.MultilingualTextType;
import ch.ehi.oereb.schemas.oereb._2_0.extractdata.MultilingualUriType;
import ch.ehi.oereb.schemas.oereb._2_0.extractdata.OfficeType;
import ch.ehi.oereb.schemas.oereb._2_0.extractdata.RealEstateDPRType;
import ch.ehi.oereb.schemas.oereb._2_0.extractdata.RealEstateTypeCodeType;
import ch.ehi.oereb.schemas.oereb._2_0.extractdata.RealEstateTypeType;
import ch.ehi.oereb.schemas.oereb._2_0.extractdata.RestrictionOnLandownershipType;
import ch.ehi.oereb.schemas.oereb._2_0.extractdata.ThemeType;
import ch.ehi.oereb.schemas.oereb._2_0.extractdata.WebReferenceType;
import ch.ehi.oereb.schemas.geometry._1_0.CoordType;
import ch.ehi.oereb.schemas.geometry._1_0.MultiSurfaceType;
import ch.ehi.oereb.schemas.geometry._1_0.PolylineType;
import ch.ehi.oereb.schemas.geometry._1_0.SurfaceType;
import ch.ehi.oereb.schemas.oereb._1_0.versioning.GetVersionsResponse;
import ch.ehi.oereb.schemas.oereb._1_0.versioning.GetVersionsResponseType;
import ch.ehi.oereb.schemas.oereb._1_0.versioning.VersionType;
import ch.so.agi.oereb.pdf4oereb.ConverterException;
import ch.so.agi.oereb.pdf4oereb.Locale;
// http://localhost:8080/extract/reduced/xml/geometry/CH693289470668


@Controller
public class OerebController {
    
    private static final String WMS_PARAM_LAYERS = "LAYERS";
    private static final String WMS_PARAM_WIDTH = "WIDTH";
    private static final String WMS_PARAM_HEIGHT = "HEIGHT";
    private static final String WMS_PARAM_DPI = "DPI";
    private static final String WMS_PARAM_BBOX = "BBOX";
    private static final String WMS_PARAM_SRS = "SRS";
    private static final String PARAM_CONST_PDF = "pdf";
    private static final String PARAM_CONST_XML = "xml";
    private static final String PARAM_CONST_URL = "url";
    private static final String PARAM_CONST_ALL_FEDERAL = "ALL_FEDERAL";
    private static final String PARAM_CONST_ALL = "ALL";
    private static final String PARAM_CONST_TRUE = "TRUE";
    private static final String PARAM_LOCALISATION = "LOCALISATION";
    private static final String PARAM_POSTALCODE = "POSTALCODE";
    private static final String PARAM_GNSS = "GNSS";
    private static final String PARAM_EN = "EN";
    private static final String PARAM_DPI = WMS_PARAM_DPI;
    private static final String PARAM_WITHIMAGES = "WITHIMAGES";
    private static final String PARAM_TOPICS = "TOPICS";
    private static final String PARAM_LANG = "LANG";
    private static final String PARAM_SIGNED = "SIGNED";
    private static final String PARAM_NUMBER = "NUMBER";
    private static final String PARAM_IDENTDN = "IDENTDN";
    private static final String PARAM_EGRID = "EGRID";
    private static final String PARAM_GEOMETRY = "GEOMETRY";
    private static final String OERBKRMVS_V2_0THEMA_THEMAGESETZ = "oerbkrmvs_v2_0thema_themagesetz";
    private static final String OERBKRMVS_V2_0KONFIGURATION_GRUNDSTUECKSARTTXT = "oerbkrmvs_v2_0konfiguration_grundstuecksarttxt";
    private static final String OEREBKRM_V2_0_LOCALISEDBLOB = "oerebkrm_v2_0_localisedblob";
    private static final String OEREBKRM_V2_0_MULTILINGUALBLOB = "oerebkrm_v2_0_multilingualblob";
    private static final String OERBKRMVS_V2_0KONFIGURATION_LOGO = "oerbkrmvs_v2_0konfiguration_logo";
    private static final String OERBKRMVS_V2_0KONFIGURATION_DOKUMENTTYPTXT = "oerbkrmvs_v2_0konfiguration_dokumenttyptxt";
    private static final String OERBKRMFR_V2_0TRANSFERSTRUKTUR_HINWEISVORSCHRIFT = "oerbkrmfr_v2_0transferstruktur_hinweisvorschrift";
    private static final String OEREBKRM_V2_0DOKUMENTE_DOKUMENT = "oerebkrm_v2_0dokumente_dokument";
    private static final String OERBKRMVS_V2_0KONFIGURATION_RECHTSSTATUSTXT = "oerbkrmvs_v2_0konfiguration_rechtsstatustxt";
    private static final String OERBKRMVS_V2_0KONFIGURATION_GLOSSAR = "oerbkrmvs_v2_0konfiguration_glossar";
    private static final String OERBKRMVS_V2_0KONFIGURATION_HAFTUNGSHINWEIS = "oerbkrmvs_v2_0konfiguration_haftungshinweis";
    private static final String OERBKRMVS_V2_0KONFIGURATION_INFORMATION = "oerbkrmvs_v2_0konfiguration_information";
    private static final String OEREBKRM_V2_0_LOCALISEDURI = "oerebkrm_v2_0_localiseduri";
    private static final String OEREBKRM_V2_0_MULTILINGUALURI = "oerebkrm_v2_0_multilingualuri";
    private static final String OERBKRMFR_V2_0TRANSFERSTRUKTUR_LEGENDEEINTRAG = "oerbkrmfr_v2_0transferstruktur_legendeeintrag";
    private static final String OEREBKRM_V2_0AMT_AMT = "oerebkrm_v2_0amt_amt";
    private static final String OERBKRMFR_V2_0TRANSFERSTRUKTUR_DARSTELLUNGSDIENST = "oerbkrmfr_v2_0transferstruktur_darstellungsdienst";
    private static final String OERBKRMFR_V2_0TRANSFERSTRUKTUR_EIGENTUMSBESCHRAENKUNG = "oerbkrmfr_v2_0transferstruktur_eigentumsbeschraenkung";
    private static final String OERBKRMFR_V2_0TRANSFERSTRUKTUR_GEOMETRIE = "oerbkrmfr_v2_0transferstruktur_geometrie";
    private static final String OEREBKRM_V2_0_THEMAREF = "oerebkrm_v2_0_themaref";
    private static final String OERBKRMVS_V2_0KONFIGURATION_MAPLAYERING = "oerbkrmvs_v2_0konfiguration_maplayering";
    private static final String OERBKRMVS_V2_0KONFIGURATION_GEMEINDEMITOEREBK = "oerbkrmvs_v2_0konfiguration_gemeindemitoerebk";
    private static final String OERBKRMVS_V2_0KONFIGURATION_GRUNDBUCHKREIS = "oerbkrmvs_v2_0konfiguration_grundbuchkreis";
    private static final String OERBKRMVS_V2_0THEMA_THEMA = "oerbkrmvs_v2_0thema_thema";
    private static final String TABLE_PLZOCH1LV95DPLZORTSCHAFT_PLZ6 = "plzoch1lv95dplzortschaft_plz6";
    private static final String TABLE_DM01VCH24LV95DGEBAEUDEADRESSEN_GEBAEUDEEINGANG = "dm01vch24lv95dgebaeudeadressen_gebaeudeeingang";
    private static final String TABLE_DM01VCH24LV95DGEBAEUDEADRESSEN_LOKALISATION = "dm01vch24lv95dgebaeudeadressen_lokalisation";
    private static final String TABLE_DM01VCH24LV95DGEBAEUDEADRESSEN_LOKALISATIONSNAME = "dm01vch24lv95dgebaeudeadressen_lokalisationsname";
    private static final String TABLE_DM01VCH24LV95DGEMEINDEGRENZEN_GEMEINDE = "dm01vch24lv95dgemeindegrenzen_gemeinde";
    private static final String TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_LIEGENSCHAFT = "dm01vch24lv95dliegenschaften_liegenschaft";
    private static final String TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_SELBSTRECHT = "dm01vch24lv95dliegenschaften_selbstrecht";
    private static final String TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_BERGWERK = "dm01vch24lv95dliegenschaften_bergwerk";
    private static final String TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_GRUNDSTUECK = "dm01vch24lv95dliegenschaften_grundstueck";

    private static final LanguageCodeType DE = LanguageCodeType.DE;
    private static final String LOGO_ENDPOINT = "logo";
    private static final String SYMBOL_ENDPOINT = "symbol";
    private static final String TMP_FOLDER_PREFIX = "oerebws";
    private static final String SERVICE_SPEC_VERSION = "extract-2.0";
    private static final String FILE_EXT_XML = ".xml";
    
    private Logger logger=org.slf4j.LoggerFactory.getLogger(this.getClass());
    private Jts2xtf24 jts2xtf = new Jts2xtf24();
    
    @Autowired
    JdbcTemplate jdbcTemplate;
    @Autowired
    NamedParameterJdbcTemplate jdbcParamTemplate; 
    
    @Autowired
    Jaxb2Marshaller marshaller;
    
    @Autowired
    ch.so.agi.oereb.pdf4oereb.Converter extractXml2pdf;
    
    @Value("${spring.datasource.url}")
    private String dburl;
    @Value("${oereb.dbschema}")
    private String dbschema;
    @Value("${oereb.cadastreAuthorityUrl}")
    private String plrCadastreAuthorityUrl;
    @Value("${oereb.webAppUrl}")
    private String webAppUrl;
    @Value("${oereb.canton:Solothurn}")
    private String plrCanton;
    @Value("${oereb.tmpdir:${java.io.tmpdir}}")
    private String oerebTmpdir;
    @Value("${oereb.minIntersection:0.001}")
    private double minIntersection;
    @Value("${oereb.dpi:300}")
    private int defaultMapDpi;
    
    @Value("${oereb.planForLandregisterMainPage}")
    private String oerebPlanForLandregisterMainPage;
    @Value("${oereb.planForLandregister}")
    private String oerebPlanForLandregister;
    
    
    private static byte[] minimalImage=Base64.decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAACklEQVR4nGMAAQAABQABDQottAAAAABJRU5ErkJggg==");

    @GetMapping("/")
    public ResponseEntity<String>  ping() {
        logger.info("env.dburl {}",dburl);
        return new ResponseEntity<String>("oereb web service",HttpStatus.OK);
    }
    @GetMapping("/logo/{id}")
    public ResponseEntity<byte[]>  getLogo(@PathVariable String id) {
        logger.info("id {}",id);
        byte image[]=getImageOrNull(id);
        if(image==null) {
            return new ResponseEntity<byte[]>(HttpStatus.NO_CONTENT);
        }
        return ResponseEntity
                .ok().header("content-disposition", "attachment; filename=" + id+".png")
                .contentLength(image.length)
                .contentType(MediaType.IMAGE_PNG).body(image);                
    }
    @GetMapping("/symbol/{id}")
    public ResponseEntity<byte[]>  getSymbol(@PathVariable String id) {
        logger.info("id {}",id);
        
        String stmt="SELECT" + 
                " symbol" + 
                " FROM "+getSchema()+"."+OERBKRMFR_V2_0TRANSFERSTRUKTUR_LEGENDEEINTRAG+" WHERE t_id=?";
        java.util.List<byte[]> baseData=jdbcTemplate.queryForList(stmt,byte[].class,Long.parseLong(id));
        byte image[]=null;
        if(baseData!=null && baseData.size()==1) {
            image=baseData.get(0);
        }
        
        if(image==null) {
            return new ResponseEntity<byte[]>(HttpStatus.NO_CONTENT);
        }
        return ResponseEntity
                .ok().header("content-disposition", "attachment; filename=" + id+".png")
                .contentLength(image.length)
                .contentType(MediaType.IMAGE_PNG).body(image);                
    }
    
    // select ST_AsText(lage) from oerebtest.dm01vch24lv95dgebaeudeadressen_lokalisationsname as lname JOIN oerebtest.dm01vch24lv95dgebaeudeadressen_lokalisation AS lokn ON lokn.t_id=lname.benannte JOIN oerebtest.dm01vch24lv95dgebaeudeadressen_gebaeudeeingang as geb ON geb.gebaeudeeingang_von=lokn.t_id where lname.atext='Hauptstrasse' and geb.hausnummer is null 
    // select  flaeche from oerebtest.plzoch1lv95dplzortschaft_plz6 where plz=3706 
    // ON ST_Intersects(gebein.lage, parcels.geometrie)
    // ON ST_Intersects(gebein.lage, plz.flaeche)
    @GetMapping("/getegrid/{format}")
    public ResponseEntity<GetEGRIDResponse>  getEgrid(@PathVariable String format, @RequestParam Map<String, String> queryParameters) {
        if(!format.equals(PARAM_CONST_XML)) {
            throw new IllegalArgumentException("unsupported format <"+format+">");
        }
        String geometryParam=queryParameters.get(PARAM_GEOMETRY);
        boolean withGeometry=geometryParam!=null?PARAM_CONST_TRUE.equalsIgnoreCase(geometryParam):false;
        String identdn=queryParameters.get(PARAM_IDENTDN);
        String en=queryParameters.get(PARAM_EN);
        String gnss=queryParameters.get(PARAM_GNSS);
        String postalcode=queryParameters.get(PARAM_POSTALCODE);
        String localisation=queryParameters.get(PARAM_LOCALISATION);
        String number=queryParameters.get(PARAM_NUMBER);
        if(identdn!=null) {
            return getEgridByNumber(withGeometry, identdn, number);
        }else if(en!=null || gnss!=null) {
            return getEgridByXY(withGeometry, en, gnss);
        }else if(postalcode!=null) {
            int plz=Integer.parseInt(postalcode);
            if(number!=null) {
                return getEgridByAddress(withGeometry, plz,localisation,number);
            }else {
                return getEgridByAddress(withGeometry, plz,localisation);
            }
        }
        throw new IllegalArgumentException("parameter IDENTDN or EN or GNSS or POSTALCODE expected");
    }
    ResponseEntity<GetEGRIDResponse>  getEgridByNumber(boolean withGeometry,String identdn,String number) {
        GetEGRIDResponseType ret= new GetEGRIDResponseType();
        ch.ehi.oereb.schemas.oereb._2_0.extract.ObjectFactory of=new ch.ehi.oereb.schemas.oereb._2_0.extract.ObjectFactory();
        List<JAXBElement<String>[]> gsList=jdbcTemplate.query(
                "SELECT egris_egrid,nummer,nbident,art AS type FROM "+getSchema()+"."+TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_GRUNDSTUECK+" WHERE nummer=? AND nbident=?", new RowMapper<JAXBElement<String>[]>() {
                    @Override
                    public JAXBElement[] mapRow(ResultSet rs, int rowNum) throws SQLException {
                        JAXBElement ret[]=new JAXBElement[5];
                        String egrid=rs.getString(1);
                        ret[0]=of.createGetEGRIDResponseTypeEgrid(egrid);
                        ret[1]=of.createGetEGRIDResponseTypeNumber(rs.getString(2));
                        ret[2]=of.createGetEGRIDResponseTypeIdentDN(rs.getString(3));
                        ret[3]=of.createGetEGRIDResponseTypeType(mapRealEstateType(rs.getString(4)));
                        if(withGeometry) {
                            MultiSurfaceType geomGml=jts2xtf.createMultiSurfaceType(getParcelGeometryByEgrid(egrid));
                            ret[4]=of.createGetEGRIDResponseTypeLimit(geomGml);
                        }
                        return ret;
                    }
                    
                },number,identdn);
        for(JAXBElement[] gs:gsList) {
            ret.getEgridAndNumberAndIdentDN().add(gs[0]);
            ret.getEgridAndNumberAndIdentDN().add(gs[1]);
            ret.getEgridAndNumberAndIdentDN().add(gs[2]);
            ret.getEgridAndNumberAndIdentDN().add(gs[3]);
            if(withGeometry) {
                ret.getEgridAndNumberAndIdentDN().add(gs[4]);
            }
        }
         return new ResponseEntity<GetEGRIDResponse>(new GetEGRIDResponse(ret),gsList.size()>0?HttpStatus.OK:HttpStatus.NO_CONTENT);
    }
    ResponseEntity<GetEGRIDResponse>  getEgridByXY(boolean  withGeometry,String xy,String gnss) {
        if(xy==null && gnss==null) {
            throw new IllegalArgumentException("parameter EN or GNSS required");
        }else if(xy!=null && gnss!=null) {
            throw new IllegalArgumentException("only one of parameters EN or GNSS is allowed");
        }
        Coordinate coord = null;
        int srid = 2056;
        double scale = 1000.0;
        if(xy!=null) {
            coord=parseCoord(xy);
            srid = 2056;
            if(coord.x<2000000.0) {
                srid=21781;
            }
        }else {
            coord=parseCoord(gnss);
            srid = 4326;
            scale=100000.0;
        }
        WKBWriter geomEncoder=new WKBWriter(2,ByteOrderValues.BIG_ENDIAN,true);
        PrecisionModel precisionModel=new PrecisionModel(scale);
        GeometryFactory geomFact=new GeometryFactory(precisionModel,srid);
        byte geom[]=geomEncoder.write(geomFact.createPoint(coord));
        // SELECT g.egris_egrid,g.nummer,g.nbident FROM oereb.dm01vch24lv95dliegenschaften_grundstueck g LEFT JOIN oereb.dm01vch24lv95dliegenschaften_liegenschaft l ON l.liegenschaft_von=g.t_id WHERE ST_DWithin(ST_GeomFromEWKT('SRID=2056;POINT( 2638242.500 1251450.000)'),l.geometrie,1.0)
        // SELECT g.egris_egrid,g.nummer,g.nbident FROM oereb.dm01vch24lv95dliegenschaften_grundstueck g LEFT JOIN oereb.dm01vch24lv95dliegenschaften_liegenschaft l ON l.liegenschaft_von=g.t_id WHERE ST_DWithin(ST_Transform(ST_GeomFromEWKT('SRID=4326;POINT( 7.94554 47.41277)'),2056),l.geometrie,1.0)
        GetEGRIDResponseType ret= new GetEGRIDResponseType();
        ch.ehi.oereb.schemas.oereb._2_0.extract.ObjectFactory of=new ch.ehi.oereb.schemas.oereb._2_0.extract.ObjectFactory();
        List<JAXBElement<String>[]> gsList=jdbcTemplate.query(
                "SELECT egris_egrid,nummer,nbident,art as type FROM "+getSchema()+"."+TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_GRUNDSTUECK+" g"
                        +" LEFT JOIN (SELECT liegenschaft_von as von, geometrie FROM "+getSchema()+"."+TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_LIEGENSCHAFT
                             +" UNION ALL SELECT selbstrecht_von as von,  geometrie FROM "+getSchema()+"."+TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_SELBSTRECHT
                             +" UNION ALL SELECT bergwerk_von as von,     geometrie FROM "+getSchema()+"."+TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_BERGWERK+") b ON b.von=g.t_id WHERE ST_DWithin(ST_Transform(?,2056),b.geometrie,1.0)"
                , new RowMapper<JAXBElement<String>[]>() {
                    @Override
                    public JAXBElement<String>[] mapRow(ResultSet rs, int rowNum) throws SQLException {
                        JAXBElement ret[]=new JAXBElement[5];
                        String egrid=rs.getString(1);
                        ret[0]=of.createGetEGRIDResponseTypeEgrid(egrid);
                        ret[1]=of.createGetEGRIDResponseTypeNumber(rs.getString(2));
                        ret[2]=of.createGetEGRIDResponseTypeIdentDN(rs.getString(3));
                        ret[3]=of.createGetEGRIDResponseTypeType(mapRealEstateType(rs.getString(4)));
                        if(withGeometry) {
                            MultiSurfaceType geomGml=jts2xtf.createMultiSurfaceType(getParcelGeometryByEgrid(egrid));
                            ret[4]=of.createGetEGRIDResponseTypeLimit(geomGml);
                        }
                        return ret;
                    }
                    
                },geom);
        for(JAXBElement[] gs:gsList) {
            ret.getEgridAndNumberAndIdentDN().add(gs[0]);
            ret.getEgridAndNumberAndIdentDN().add(gs[1]);
            ret.getEgridAndNumberAndIdentDN().add(gs[2]);
            ret.getEgridAndNumberAndIdentDN().add(gs[3]);
            if(withGeometry) {
                ret.getEgridAndNumberAndIdentDN().add(gs[4]);
            }
        }
         return new ResponseEntity<GetEGRIDResponse>(new GetEGRIDResponse(ret),gsList.size()>0?HttpStatus.OK:HttpStatus.NO_CONTENT);
    }
    
    private ResponseEntity<GetEGRIDResponse>  getEgridByAddress(boolean withGeometry, int postalcode,String localisation,String number) {
        logger.debug("postalcode {}",postalcode);
        logger.debug("localisation {}",localisation);
        logger.debug("number {}",number);
        GetEGRIDResponseType ret= new GetEGRIDResponseType();
        ch.ehi.oereb.schemas.oereb._2_0.extract.ObjectFactory of=new ch.ehi.oereb.schemas.oereb._2_0.extract.ObjectFactory();
        String stmt="SELECT DISTINCT egris_egrid,nummer,nbident, art as type FROM "+getSchema()+"."+TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_GRUNDSTUECK+" as g"
                +" JOIN ("
                + "(SELECT liegenschaft_von as von, geometrie FROM "+getSchema()+"."+TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_LIEGENSCHAFT
                    +" UNION ALL SELECT selbstrecht_von as von,  geometrie FROM "+getSchema()+"."+TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_SELBSTRECHT
                    +" UNION ALL SELECT bergwerk_von as von,     geometrie FROM "+getSchema()+"."+TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_BERGWERK+") as a "
                            + " JOIN (select lage from (select lage from "+getSchema()+"."+TABLE_DM01VCH24LV95DGEBAEUDEADRESSEN_LOKALISATIONSNAME+" as lname " 
                            + " JOIN "+getSchema()+"."+TABLE_DM01VCH24LV95DGEBAEUDEADRESSEN_LOKALISATION+" AS lokn ON lokn.t_id=lname.benannte "
                            + " JOIN "+getSchema()+"."+TABLE_DM01VCH24LV95DGEBAEUDEADRESSEN_GEBAEUDEEINGANG+" as geb ON geb.gebaeudeeingang_von=lokn.t_id " 
                            + " where (lname.atext=? or lname.kurztext=?)and geb.hausnummer=? " 
                            + ") as adr "
                            + " JOIN (select  flaeche from "+getSchema()+"."+TABLE_PLZOCH1LV95DPLZORTSCHAFT_PLZ6+" where plz=?) as plz ON ST_Intersects(adr.lage, plz.flaeche) " 
                            + ") as ladr ON ST_Intersects(ladr.lage,a.geometrie)"
            + ") as b ON b.von=g.t_id";
        List<JAXBElement<String>[]> gsList=jdbcTemplate.query(
                stmt
                , new RowMapper<JAXBElement<String>[]>() {
                    @Override
                    public JAXBElement<String>[] mapRow(ResultSet rs, int rowNum) throws SQLException {
                        JAXBElement ret[]=new JAXBElement[3];
                        String egrid=rs.getString(1);
                        ret[0]=of.createGetEGRIDResponseTypeEgrid(egrid);
                        ret[1]=of.createGetEGRIDResponseTypeNumber(rs.getString(2));
                        ret[2]=of.createGetEGRIDResponseTypeIdentDN(rs.getString(3));
                        ret[3]=of.createGetEGRIDResponseTypeType(mapRealEstateType(rs.getString(4)));
                        if(withGeometry) {
                            MultiSurfaceType geomGml=jts2xtf.createMultiSurfaceType(getParcelGeometryByEgrid(egrid));
                            ret[4]=of.createGetEGRIDResponseTypeLimit(geomGml);
                        }
                        return ret;
                    }
                    
                },localisation,localisation,number,postalcode);
        for(JAXBElement[] gs:gsList) {
            ret.getEgridAndNumberAndIdentDN().add(gs[0]);
            ret.getEgridAndNumberAndIdentDN().add(gs[1]);
            ret.getEgridAndNumberAndIdentDN().add(gs[2]);
            ret.getEgridAndNumberAndIdentDN().add(gs[3]);
            if(withGeometry) {
                ret.getEgridAndNumberAndIdentDN().add(gs[4]);
            }
        }
         return new ResponseEntity<GetEGRIDResponse>(new GetEGRIDResponse(ret),gsList.size()>0?HttpStatus.OK:HttpStatus.NO_CONTENT);
    }
    private ResponseEntity<GetEGRIDResponse>  getEgridByAddress(boolean withGeometry,int postalcode,String localisation) {
        logger.debug("postalcode {}",postalcode);
        logger.debug("localisation {}",localisation);
        GetEGRIDResponseType ret= new GetEGRIDResponseType();
        ch.ehi.oereb.schemas.oereb._2_0.extract.ObjectFactory of=new ch.ehi.oereb.schemas.oereb._2_0.extract.ObjectFactory();
        String stmt="SELECT DISTINCT egris_egrid,nummer,nbident,art as type FROM "+getSchema()+"."+TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_GRUNDSTUECK+" as g"
                +" JOIN ("
                + "(SELECT liegenschaft_von as von, geometrie FROM "+getSchema()+"."+TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_LIEGENSCHAFT
                    +" UNION ALL SELECT selbstrecht_von as von,  geometrie FROM "+getSchema()+"."+TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_SELBSTRECHT
                    +" UNION ALL SELECT bergwerk_von as von,     geometrie FROM "+getSchema()+"."+TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_BERGWERK+") as a "
                            + " JOIN (select lage from (select lage from "+getSchema()+"."+TABLE_DM01VCH24LV95DGEBAEUDEADRESSEN_LOKALISATIONSNAME+" as lname " 
                            + " JOIN "+getSchema()+"."+TABLE_DM01VCH24LV95DGEBAEUDEADRESSEN_LOKALISATION+" AS lokn ON lokn.t_id=lname.benannte "
                            + " JOIN "+getSchema()+"."+TABLE_DM01VCH24LV95DGEBAEUDEADRESSEN_GEBAEUDEEINGANG+" as geb ON geb.gebaeudeeingang_von=lokn.t_id " 
                            + " where (lname.atext=? or lname.kurztext=?)and geb.hausnummer is null " 
                            + ") as adr "
                            + " JOIN (select  flaeche from "+getSchema()+"."+TABLE_PLZOCH1LV95DPLZORTSCHAFT_PLZ6+" where plz=?) as plz ON ST_Intersects(adr.lage, plz.flaeche) " 
                            + ") as ladr ON ST_Intersects(ladr.lage,a.geometrie)"
            + ") as b ON b.von=g.t_id";
        List<JAXBElement<String>[]> gsList=jdbcTemplate.query(
                stmt
                , new RowMapper<JAXBElement<String>[]>() {
                    @Override
                    public JAXBElement<String>[] mapRow(ResultSet rs, int rowNum) throws SQLException {
                        JAXBElement ret[]=new JAXBElement[3];
                        String egrid=rs.getString(1);
                        ret[0]=of.createGetEGRIDResponseTypeEgrid(egrid);
                        ret[1]=of.createGetEGRIDResponseTypeNumber(rs.getString(2));
                        ret[2]=of.createGetEGRIDResponseTypeIdentDN(rs.getString(3));
                        ret[3]=of.createGetEGRIDResponseTypeType(mapRealEstateType(rs.getString(4)));
                        if(withGeometry) {
                            MultiSurfaceType geomGml=jts2xtf.createMultiSurfaceType(getParcelGeometryByEgrid(egrid));
                            ret[4]=of.createGetEGRIDResponseTypeLimit(geomGml);
                        }
                        return ret;
                    }
                    
                },localisation,localisation,postalcode);
        for(JAXBElement[] gs:gsList) {
            ret.getEgridAndNumberAndIdentDN().add(gs[0]);
            ret.getEgridAndNumberAndIdentDN().add(gs[1]);
            ret.getEgridAndNumberAndIdentDN().add(gs[2]);
            ret.getEgridAndNumberAndIdentDN().add(gs[3]);
            if(withGeometry) {
                ret.getEgridAndNumberAndIdentDN().add(gs[4]);
            }
        }
         return new ResponseEntity<GetEGRIDResponse>(new GetEGRIDResponse(ret),gsList.size()>0?HttpStatus.OK:HttpStatus.NO_CONTENT);
    }
    
    @GetMapping(value="/extract/{format}",consumes=MediaType.ALL_VALUE,produces = {MediaType.APPLICATION_PDF_VALUE,MediaType.APPLICATION_XML_VALUE})
    public ResponseEntity<?>  getExtract(@PathVariable String format,@RequestParam Map<String, String> queryParameters) {
        String geometryParam=queryParameters.get(PARAM_GEOMETRY);
        boolean withGeometry=geometryParam!=null?PARAM_CONST_TRUE.equalsIgnoreCase(geometryParam):false;
        String egrid=queryParameters.get(PARAM_EGRID);
        String identdn=queryParameters.get(PARAM_IDENTDN);
        String number=queryParameters.get(PARAM_NUMBER);
        String signed=queryParameters.get(PARAM_SIGNED);
        String lang=queryParameters.get(PARAM_LANG);
        String topics=queryParameters.get(PARAM_TOPICS);
        String withImages=queryParameters.get(PARAM_WITHIMAGES);
        String dpiParam=queryParameters.get(PARAM_DPI);
        int dpi=dpiParam!=null?Integer.parseInt(dpiParam):defaultMapDpi;
        if(format.equalsIgnoreCase(PARAM_CONST_URL)) {
            return getExtractRedirect(egrid,identdn,number);
        }else if(egrid!=null) {
            if(withGeometry) {
                return getExtractWithGeometryByEgrid(format,egrid,lang,topics,withImages,dpi);
            }
            return getExtractWithoutGeometryByEgrid(format,egrid,lang,topics,withImages,dpi);
        }else {
            if(withGeometry) {
                return getExtractWithGeometryByNumber(format,identdn,number,lang,topics,withImages,dpi);
            }
            return getExtractWithoutGeometryByNumber(format,identdn,number,lang,topics,withImages,dpi);
        }
    }
                
    ResponseEntity<?>  getExtractWithGeometryByEgrid(String format,String egrid,String lang,String topics,String withImagesParam,int dpi) {
        if(!format.equals(PARAM_CONST_XML) && !format.equals(PARAM_CONST_PDF)) {
            throw new IllegalArgumentException("unsupported format <"+format+">");
        }
        Grundstueck parcel=getParcelByEgrid(egrid);
        if(parcel==null) {
            return new ResponseEntity<Object>(HttpStatus.NO_CONTENT);
        }
        java.sql.Date basedataDate=getBasedatadateOfMunicipality(parcel.getBfsNr());
        if(basedataDate==null) {
            // non unlocked municipality
            return new ResponseEntity<Object>(HttpStatus.NO_CONTENT);
        }
        boolean withGeometry = true;
        boolean withImages = withImagesParam==null?false:PARAM_CONST_TRUE.equalsIgnoreCase(withImagesParam);
        if(format.equals(PARAM_CONST_PDF)) {
            withImages = true;
            withGeometry = true;
        }
        Extract extract=createExtract(parcel.getEgrid(),parcel,basedataDate,withGeometry,lang,topics,withImages,dpi);
        
        GetExtractByIdResponseType response=new GetExtractByIdResponseType();
        response.setExtract(extract);
        GetExtractByIdResponse responseEle=new GetExtractByIdResponse(response);
        
        if(format.equals(PARAM_CONST_PDF)) {
            return createExtractAsPdf(parcel, responseEle);
        }
        return new ResponseEntity<GetExtractByIdResponse>(responseEle,HttpStatus.OK);
    }
    private ResponseEntity<?> createExtractAsPdf(Grundstueck parcel, GetExtractByIdResponse responseEle) {
        java.io.File tmpFolder=new java.io.File(oerebTmpdir,TMP_FOLDER_PREFIX+Thread.currentThread().getId());
        if(!tmpFolder.exists()) {
            tmpFolder.mkdirs();
        }
        logger.info("tmpFolder {}",tmpFolder.getAbsolutePath());
        java.io.File tmpExtractFile=new java.io.File(tmpFolder,parcel.getEgrid()+FILE_EXT_XML);
        marshaller.marshal(responseEle,new javax.xml.transform.stream.StreamResult(tmpExtractFile));
        try {
            java.io.File pdfFile=extractXml2pdf.runXml2Pdf(tmpExtractFile.getAbsolutePath(), tmpFolder.getAbsolutePath(), Locale.DE);
            pdfFile.getName();
            /*
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_PDF);
            headers.add("Access-Control-Allow-Origin", "*");
            //headers.add("Access-Control-Allow-Methods", "GET, POST, PUT");
            headers.add("Access-Control-Allow-Headers", "Content-Type");
            headers.add("Content-Disposition", "filename=" + pdfFilename);
            headers.add("Cache-Control", "no-cache, no-store, must-revalidate");
            headers.add("Pragma", "no-cache");
            headers.add("Expires", "0");
            headers.setContentLength(pdfFile.length());
            return new ResponseEntity<java.io.FileInputStream>(
                    new java.io.FileInputStream(pdfFile), headers, HttpStatus.OK);                
            */
            java.io.InputStream is = new java.io.FileInputStream(pdfFile);
            return ResponseEntity
                    .ok().header("content-disposition", "attachment; filename=" + pdfFile.getName())
                    .contentLength(pdfFile.length())
                    .contentType(MediaType.APPLICATION_PDF).body(new InputStreamResource(is));                
        } catch (ConverterException e) {
            throw new IllegalStateException(e);
        } catch (FileNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }    

    private ResponseEntity<?>  getExtractWithoutGeometryByEgrid(String format,String egrid,String lang,String topics,String withImagesParam,int dpi) {
        if(!format.equals(PARAM_CONST_XML) && !format.equals(PARAM_CONST_PDF)) {
            throw new IllegalArgumentException("unsupported format <"+format+">");
        }
        Grundstueck parcel=getParcelByEgrid(egrid);
        if(parcel==null) {
            return new ResponseEntity<Object>(HttpStatus.NO_CONTENT);
        }
        java.sql.Date basedataDate=getBasedatadateOfMunicipality(parcel.getBfsNr());
        if(basedataDate==null) {
            // non unlocked municipality
            return new ResponseEntity<Object>(HttpStatus.NO_CONTENT);
        }

        boolean withGeometry = false;
        boolean withImages = withImagesParam==null?false:PARAM_CONST_TRUE.equalsIgnoreCase(withImagesParam);
        if(format.equals(PARAM_CONST_PDF)) {
            withImages = true;
            withGeometry = true;
        }
        Extract extract=createExtract(parcel.getEgrid(),parcel,basedataDate,withGeometry,lang,topics,withImages,dpi);
        
        GetExtractByIdResponseType response=new GetExtractByIdResponseType();
        response.setExtract(extract);
        GetExtractByIdResponse responseEle=new GetExtractByIdResponse(response);
        
        if(format.equals(PARAM_CONST_PDF)) {
            return createExtractAsPdf(parcel, responseEle);
        }
        return new ResponseEntity<GetExtractByIdResponse>(responseEle,HttpStatus.OK);
    }    
    private ResponseEntity<?>  getExtractWithGeometryByNumber(String format,String identdn,String number,String lang,String topics,String withImagesParam,int dpi) {
        if(!format.equals(PARAM_CONST_XML) && !format.equals(PARAM_CONST_PDF)) {
            throw new IllegalArgumentException("unsupported format <"+format+">");
        }
        Grundstueck parcel=getParcelByNumber(identdn,number);
        if(parcel==null) {
            return new ResponseEntity<Object>(HttpStatus.NO_CONTENT);
        }
        java.sql.Date basedataDate=getBasedatadateOfMunicipality(parcel.getBfsNr());
        if(basedataDate==null) {
            // non unlocked municipality
            return new ResponseEntity<Object>(HttpStatus.NO_CONTENT);
        }

        boolean withGeometry = true;
        boolean withImages = withImagesParam==null?false:PARAM_CONST_TRUE.equalsIgnoreCase(withImagesParam);
        if(format.equals(PARAM_CONST_PDF)) {
            withImages = true;
            withGeometry = true;
        }
        Extract extract=createExtract(parcel.getEgrid(),parcel,basedataDate,withGeometry,lang,topics,withImages,dpi);
        
        GetExtractByIdResponseType response=new GetExtractByIdResponseType();
        response.setExtract(extract);
        GetExtractByIdResponse responseEle=new GetExtractByIdResponse(response);
        
        if(format.equals(PARAM_CONST_PDF)) {
            return createExtractAsPdf(parcel, responseEle);
        }
        return new ResponseEntity<GetExtractByIdResponse>(responseEle,HttpStatus.OK);
    }    
    private ResponseEntity<?> getExtractRedirect(String egridParam, String identdn, String number) {
        String egrid=verifyEgrid(egridParam, identdn, number);
        if(egrid==null) {
            return new ResponseEntity<Object>(HttpStatus.NO_CONTENT);
        }
        HttpHeaders headers=new HttpHeaders();
        headers.add(HttpHeaders.LOCATION, getWebAppUrl(egrid));
        ResponseEntity<Object> ret=new ResponseEntity<Object>(headers,HttpStatus.SEE_OTHER);
        return ret;
    }

    private ResponseEntity<?>  getExtractWithoutGeometryByNumber(String format,String identdn,String number,String lang,String topics,String withImagesParam,int dpi) {
        if(!format.equals(PARAM_CONST_XML) && !format.equals(PARAM_CONST_PDF)) {
            throw new IllegalArgumentException("unsupported format <"+format+">");
        }
        Grundstueck parcel=getParcelByNumber(identdn,number);
        if(parcel==null) {
            return new ResponseEntity<Object>(HttpStatus.NO_CONTENT);
        }
        java.sql.Date basedataDate=getBasedatadateOfMunicipality(parcel.getBfsNr());
        if(basedataDate==null) {
            // non unlocked municipality
            return new ResponseEntity<Object>(HttpStatus.NO_CONTENT);
        }

        boolean withGeometry = false;
        boolean withImages = withImagesParam==null?false:PARAM_CONST_TRUE.equalsIgnoreCase(withImagesParam);
        if(format.equals(PARAM_CONST_PDF)) {
            withImages = true;
            withGeometry = true;
        }
        Extract extract=createExtract(parcel.getEgrid(),parcel,basedataDate,withGeometry,lang,topics,withImages,dpi);
        
        GetExtractByIdResponseType response=new GetExtractByIdResponseType();
        response.setExtract(extract);
        GetExtractByIdResponse responseEle=new GetExtractByIdResponse(response);
        
        if(format.equals(PARAM_CONST_PDF)) {
            return createExtractAsPdf(parcel, responseEle);
        }
        return new ResponseEntity<GetExtractByIdResponse>(responseEle,HttpStatus.OK);
    }    
    @GetMapping("/capabilities/{format}")
    public @ResponseBody  GetCapabilitiesResponse getCapabilities(@PathVariable String format) {
        if(!format.equals(PARAM_CONST_XML)) {
            throw new IllegalArgumentException("unsupported format <"+format+">");
        }
        GetCapabilitiesResponseType ret=new GetCapabilitiesResponseType();
        
        // Liste der vorhandenen OeREB-Katasterthemen 
        //   inkl. Kantons- und Gemeindethemen
        //   aber ohne Sub-Themen 
        List<TopicCode> allTopicsOfThisCadastre = getAllTopicsOfThisCadastre();
        Set<TopicCode> allTopics=new HashSet<TopicCode>();
        for(TopicCode topic:allTopicsOfThisCadastre) {
            allTopics.add(topic.getMainTopic());
        }
        allTopicsOfThisCadastre=new ArrayList<TopicCode>(allTopics);
        Map<TopicCode,Integer> topicOrdering=getTopicOrdering();
        setThemes(ret.getTopic(),sortTopics(allTopicsOfThisCadastre,topicOrdering));
        
        // Liste der vorhandenen Gemeinden;
        List<Integer> gemeinden=jdbcTemplate.query(
                "SELECT bfsnr FROM "+getSchema()+".dm01vch24lv95dgemeindegrenzen_gemeinde", new RowMapper<Integer>() {
                    @Override
                    public Integer mapRow(ResultSet rs, int rowNum) throws SQLException {
                        return rs.getInt(1);
                    }
                    
                });
        ret.getMunicipality().addAll(gemeinden);
        // Liste der vorhandenen FLAVOURs;
        ret.getFlavour().add("REDUCED");
        // Liste der unterstuetzten Sprachen (2 stellige ISO Codes);
        ret.getLanguage().add("de");
        // Liste der unterstuetzten CRS.
        ret.getCrs().add("2056");
        return new GetCapabilitiesResponse(ret);
    }

    @GetMapping("/versions/{format}")
    public @ResponseBody  GetVersionsResponse getVersions(@PathVariable String format) {
        if(!format.equals(PARAM_CONST_XML)) {
            throw new IllegalArgumentException("unsupported format <"+format+">");
        }
        GetVersionsResponseType ret=new GetVersionsResponseType();
        VersionType ver=new VersionType();
        ver.setVersion(SERVICE_SPEC_VERSION);
        ret.getSupportedVersion().add(ver);
        return new GetVersionsResponse(ret);
    }
    
    private Extract createExtract(String egrid, Grundstueck parcel, java.sql.Date basedataDate,boolean withGeometry, String lang, String requestedTopicsAsText, boolean withImages,int dpi) {
        ExtractType extract=new ExtractType();
        logger.info("timezone id {}",TimeZone.getDefault().getID());
        XMLGregorianCalendar today=createXmlDate(new java.util.Date());
        extract.setCreationDate(today);
        extract.setExtractIdentifier(UUID.randomUUID().toString());
        List<TopicCode> requestedTopics=parseParameterTopics(requestedTopicsAsText);
        // Grundstueck
        final Geometry parcelGeom = parcel.getGeometrie();
        Envelope bbox = getMapBBOX(parcelGeom);
        setParcel(extract,egrid,parcel,bbox,withGeometry,withImages,dpi);
        int bfsNr=extract.getRealEstate().getMunicipalityCode();
        // freigeschaltete Themen in der betroffenen Gemeinde
        List<TopicCode> availableTopics=getTopicsOfMunicipality(bfsNr);
        List<TopicCode> queryTopics=new ArrayList<TopicCode>();
        for(TopicCode availableTopic:availableTopics) {
            TopicCode mainTopic=availableTopic.getMainTopic();
            if(requestedTopics.contains(mainTopic) || requestedTopics.contains(availableTopic)) {
                queryTopics.add(availableTopic);
            }
        }

        Map<TopicCode,Integer> topicOrdering=getTopicOrdering();
        
        List<TopicCode> concernedTopics=new ArrayList<TopicCode>();
        List<RestrictionOnLandownershipType> rests=getRestrictions(parcelGeom,bbox,withGeometry,withImages,dpi,queryTopics,concernedTopics);
        rests.sort(new Comparator<RestrictionOnLandownershipType>() {

            @Override
            public int compare(RestrictionOnLandownershipType o1, RestrictionOnLandownershipType o2) {
                Integer o1Code=topicOrdering.get(new TopicCode(o1.getTheme().getCode(),o1.getTheme().getSubCode()));
                Integer o2Code=topicOrdering.get(new TopicCode(o2.getTheme().getCode(),o2.getTheme().getSubCode()));
                return o1Code.compareTo(o2Code);
            }
            
        });
        for(RestrictionOnLandownershipType rest:rests) {
            extract.getRealEstate().getRestrictionOnLandownership().add(rest);
        }
        
        // Themen
        List<TopicCode> themeWithoutData=new ArrayList<TopicCode>();
        for(TopicCode requestedTopic:requestedTopics) {
            TopicCode mainTopic=requestedTopic.getMainTopic();
            if(!themeWithoutData.contains(mainTopic)) {
                themeWithoutData.add(mainTopic);
            }
        }
        for(TopicCode availableTopic:availableTopics) {
            TopicCode mainTopic=availableTopic.getMainTopic();
            themeWithoutData.remove(mainTopic);
        }
        List<TopicCode> notConcernedTopics=new ArrayList<TopicCode>();
        notConcernedTopics.addAll(availableTopics);
        for(TopicCode concernedTopic:concernedTopics) {
            if(availableTopics.contains(concernedTopic)) {
                notConcernedTopics.remove(concernedTopic);
            }
        }
        setThemes(extract.getConcernedTheme(), sortTopics(concernedTopics,topicOrdering));
        setThemes(extract.getNotConcernedTheme(), sortTopics(notConcernedTopics,topicOrdering));
        setThemes(extract.getThemeWithoutData(), sortTopics(themeWithoutData,topicOrdering));
        // Logos
        if(withImages) {
            extract.setLogoPLRCadastre(getImage("ch.plr"));
            extract.setFederalLogo(getImage("ch"));
            extract.setCantonalLogo(getImage("ch."+extract.getRealEstate().getCanton().name()));
            extract.setMunicipalityLogo(getImage("ch."+extract.getRealEstate().getMunicipalityCode()));
        }else {
            extract.setLogoPLRCadastreRef(getLogoRef("ch.plr"));
            extract.setFederalLogoRef(getLogoRef("ch"));
            extract.setCantonalLogoRef(getLogoRef("ch."+extract.getRealEstate().getCanton().name()));
            extract.setMunicipalityLogoRef(getLogoRef("ch."+extract.getRealEstate().getMunicipalityCode()));
        }
        // Text

        setBaseData(extract,basedataDate);
        setGeneralInformation(extract);
        setExclusionOfLiability(extract);
        setGlossary(extract);
        // Oereb-Amt
        OfficeType plrCadastreAuthority = new OfficeType();
        plrCadastreAuthority.setName(createMultilingualTextType("OEREB-Katasteramt"));
        plrCadastreAuthority.setOfficeAtWeb(createMultilingualUri(plrCadastreAuthorityUrl));

        setOffice(plrCadastreAuthority);
        extract.setPLRCadastreAuthority(plrCadastreAuthority);
        
        return new Extract(extract);
    }

    private XMLGregorianCalendar createXmlDate(Date date) {
        try {
            GregorianCalendar gdate=new GregorianCalendar();
            gdate.setTime(date);
            XMLGregorianCalendar today = DatatypeFactory.newInstance().newXMLGregorianCalendar(gdate);
            return today;
        } catch (DatatypeConfigurationException e) {
            throw new IllegalStateException(e);
        }
    }
    private MultilingualUriType createMultilingualUri(String ref) {
        if(ref==null || ref.length()==0) {
            return null;
        }
        LocalisedUriType uri=new LocalisedUriType();
        //uri.setLanguage(value);
        uri.setText(ref);
        MultilingualUriType ret=new MultilingualUriType();
        ret.getLocalisedText().add(uri);
        return ret;
    }
    
    private String getWebAppUrl(String egrid) {
        return webAppUrl+egrid;
    }
    private String getSymbolRef(String id) {
        return ServletUriComponentsBuilder.fromCurrentContextPath().pathSegment(SYMBOL_ENDPOINT).pathSegment(id).build().toUriString();
    }
    private String getLogoRef(String id) {
        return ServletUriComponentsBuilder.fromCurrentContextPath().pathSegment(LOGO_ENDPOINT).pathSegment(id).build().toUriString();
    }
    private byte[] getImage(String code) {
        byte[] ret=getImageOrNull(code);
        if(ret!=null) {
            return ret;
        }
        return minimalImage;
    }
    private byte[] getImageOrNull(String code) {
        java.util.List<Map<String,Object>> baseData=jdbcTemplate.queryForList(
                "SELECT b.ablob as logo,b_de.ablob as logo_de FROM "+getSchema()+"."+OERBKRMVS_V2_0KONFIGURATION_LOGO+" AS l"
                +" LEFT JOIN "+getSchema()+"."+OEREBKRM_V2_0_MULTILINGUALBLOB+" AS mb ON mb.oerbkrmvs_vnfgrtn_logo_bild=l.t_id"
                +" LEFT JOIN (SELECT ablob,oerbkrm_v2_mltlnglblob_localisedblob FROM "+getSchema()+"."+OEREBKRM_V2_0_LOCALISEDBLOB+" WHERE alanguage IS NULL) AS b ON b.oerbkrm_v2_mltlnglblob_localisedblob=mb.t_id"
                +" LEFT JOIN "+getSchema()+"."+OEREBKRM_V2_0_MULTILINGUALBLOB+" AS mb_de ON mb_de.oerbkrmvs_vnfgrtn_logo_bild=l.t_id"
                +" LEFT JOIN (SELECT ablob,oerbkrm_v2_mltlnglblob_localisedblob FROM "+getSchema()+"."+OEREBKRM_V2_0_LOCALISEDBLOB+" WHERE alanguage='de') AS b_de ON b_de.oerbkrm_v2_mltlnglblob_localisedblob=mb_de.t_id"
                +" WHERE l.acode=?",code);
        if(baseData!=null && baseData.size()==1) {
            byte[] logo=(byte[])baseData.get(0).get("logo");
            byte[] logo_de=(byte[])baseData.get(0).get("logo_de");
            if(logo_de!=null) {
                return logo_de;
            }
            return logo;
        }
        return null;
    }

    private void setOffice(OfficeType office) {
        java.util.Map<String,Object> baseData=null;
        try {
            String sqlStmt=
                    "SELECT aname,aname_de,ea_lu.atext as amtimweb,ea_lu_de.atext as amtimweb_de, auid,zeile1,zeile2,strasse,hausnr,plz,ort FROM "+getSchema()+"."+OEREBKRM_V2_0AMT_AMT+" AS ea"
                            +" LEFT JOIN "+getSchema()+"."+OEREBKRM_V2_0_MULTILINGUALURI+" as ea_mu ON ea.t_id = ea_mu.oerebkrm_v2_0amt_amt_amtimweb"+" LEFT JOIN (SELECT atext,oerbkrm_v2__mltlngluri_localisedtext FROM "+getSchema()+"."+OEREBKRM_V2_0_LOCALISEDURI+" WHERE alanguage IS NULL) as ea_lu ON ea_mu.t_id = ea_lu.oerbkrm_v2__mltlngluri_localisedtext"
                            +" LEFT JOIN "+getSchema()+"."+OEREBKRM_V2_0_MULTILINGUALURI+" as ea_mu_de ON ea.t_id = ea_mu_de.oerebkrm_v2_0amt_amt_amtimweb"+" LEFT JOIN (SELECT atext,oerbkrm_v2__mltlngluri_localisedtext FROM "+getSchema()+"."+OEREBKRM_V2_0_LOCALISEDURI+" WHERE alanguage='de') as ea_lu_de ON ea_mu_de.t_id = ea_lu_de.oerbkrm_v2__mltlngluri_localisedtext"
                            +" WHERE ea_lu.atext=? OR ea_lu_de.atext=?";
            logger.info("stmt {} ",sqlStmt);
            String uri=getUri(office.getOfficeAtWeb());
            baseData=jdbcTemplate.queryForMap(sqlStmt
            ,uri,uri);
        }catch(EmptyResultDataAccessException ex) {
            ; // ignore if no record found
        }
        if(baseData!=null) {
            office.setName(createMultilingualTextType(baseData, "aname"));
            office.setOfficeAtWeb(createMultilingualUri(baseData, "amtimweb"));
            office.setUID((String) baseData.get("auid"));
            office.setLine1((String) baseData.get("zeile2"));
            office.setLine2((String) baseData.get("zeile1"));
            office.setStreet((String) baseData.get("strasse"));
            office.setNumber((String) baseData.get("hausnr"));
            office.setPostalCode((String) baseData.get("plz"));
            office.setCity((String) baseData.get("ort"));
        }
    }

    private String getUri(MultilingualUriType multilingualUri) {
        if(multilingualUri==null) {
            return null;
        }
        for(LocalisedUriType uri:multilingualUri.getLocalisedText()) {
            return uri.getText();
        }
        return null;
    }
    private void setGlossary(ExtractType extract) {
        java.util.List<java.util.Map<String,Object>> baseDataList=jdbcTemplate.queryForList(
                "SELECT titel_de,titel_fr,titel_it,titel_rm,titel_en,inhalt_de,inhalt_fr,inhalt_it,inhalt_rm,inhalt_en FROM "+getSchema()+"."+OERBKRMVS_V2_0KONFIGURATION_GLOSSAR);
        for(java.util.Map<String,Object> baseData:baseDataList) {
            MultilingualMTextType content = createMultilingualMTextType(baseData,"inhalt");
            MultilingualTextType title = createMultilingualTextType(baseData,"titel");
            GlossaryType glossary=new GlossaryType();
            glossary.setContent(content);
            glossary.setTitle(title);
            extract.getGlossary().add(glossary);
        }
    }

    private void setExclusionOfLiability(ExtractType extract) {
        java.util.List<java.util.Map<String,Object>> baseDataList=jdbcTemplate.queryForList(
                "SELECT titel_de,titel_fr,titel_it,titel_rm,titel_en,inhalt_de,inhalt_fr,inhalt_it,inhalt_rm,inhalt_en FROM "+getSchema()+"."+OERBKRMVS_V2_0KONFIGURATION_HAFTUNGSHINWEIS+" ORDER BY auszugindex");
        for(java.util.Map<String,Object> baseData:baseDataList) {
            MultilingualMTextType content = createMultilingualMTextType(baseData,"inhalt");
            MultilingualTextType title = createMultilingualTextType(baseData,"titel");
            DisclaimerType exclOfLiab=new DisclaimerType();
            exclOfLiab.setContent(content);
            exclOfLiab.setTitle(title);
            extract.getDisclaimer().add(exclOfLiab);
        }
    }

    private void setGeneralInformation(ExtractType extract) {
        java.util.Map<String,Object> baseData=jdbcTemplate.queryForMap(
                "SELECT inhalt_de,inhalt_fr,inhalt_it,inhalt_rm,inhalt_en FROM "+getSchema()+"."+OERBKRMVS_V2_0KONFIGURATION_INFORMATION);
        Map<String,String> params=new HashMap<String,String>();
        params.put("canton", plrCanton);
        extract.getGeneralInformation().add(createMultilingualMTextType(baseData,"inhalt",params));
    }

    private void setBaseData(ExtractType extract,java.sql.Date basedataDate) {
        XMLGregorianCalendar basedataXml=createXmlDate(basedataDate);
        extract.setUpdateDateCS(basedataXml);
    }

    private MultilingualMTextType createMultilingualMTextType(Map<String, Object> baseData,String prefix) {
        MultilingualMTextType ret=new MultilingualMTextType();
        for(LanguageCodeType lang:LanguageCodeType.values()) {
            String txt=(String)baseData.get(prefix+"_"+lang.value());
            if(txt!=null && txt.length()>0) {
                LocalisedMTextType lTxt= new LocalisedMTextType();
                lTxt.setLanguage(lang);
                lTxt.setText(txt);
                ret.getLocalisedText().add(lTxt);
            }
        }
        return ret;
    }
    private MultilingualMTextType createMultilingualMTextType(Map<String, Object> baseData,String prefix,Map<String,String> params) {
        MultilingualMTextType ret=new MultilingualMTextType();
        for(LanguageCodeType lang:LanguageCodeType.values()) {
            String txt=(String)baseData.get(prefix+"_"+lang.value());
            if(txt!=null && txt.length()>0) {
                if(params!=null) {
                    for(String key:params.keySet()) {
                        String param="${"+key+"}";
                        int pos=txt.indexOf(param);
                        if(pos>-1) {
                            txt=txt.substring(0, pos)+params.get(key)+txt.substring(pos+param.length());
                        }
                    }
                }
                LocalisedMTextType lTxt= new LocalisedMTextType();
                lTxt.setLanguage(lang);
                lTxt.setText(txt);
                ret.getLocalisedText().add(lTxt);
            }
        }
        return ret;
    }
    private MultilingualTextType createMultilingualTextType(Map<String, Object> baseData,String prefix) {
        MultilingualTextType ret=new MultilingualTextType();
        {
            String txt=(String)baseData.get(prefix);
            if(txt!=null) {
                LocalisedTextType lTxt= new LocalisedTextType();
                lTxt.setText(txt);
                ret.getLocalisedText().add(lTxt);
            }
        }
        for(LanguageCodeType lang:LanguageCodeType.values()) {
            String txt=(String)baseData.get(prefix+"_"+lang.value());
            if(txt!=null) {
                LocalisedTextType lTxt= new LocalisedTextType();
                lTxt.setLanguage(lang);
                lTxt.setText(txt);
                ret.getLocalisedText().add(lTxt);
            }
        }
        return ret;
    }
    private MultilingualUriType createMultilingualUri(Map<String, Object> baseData,String prefix) {
        MultilingualUriType ret=new MultilingualUriType();
        {
            String txt=(String)baseData.get(prefix);
            if(txt!=null) {
                LocalisedUriType lTxt= new LocalisedUriType();
                lTxt.setText(txt);
                ret.getLocalisedText().add(lTxt);
            }
        }
        for(LanguageCodeType lang:LanguageCodeType.values()) {
            String txt=(String)baseData.get(prefix+"_"+lang.value());
            if(txt!=null) {
                LocalisedUriType lTxt= new LocalisedUriType();
                lTxt.setLanguage(lang);
                lTxt.setText(txt);
                ret.getLocalisedText().add(lTxt);
            }
        }
        return ret;
    }
    private MultilingualMTextType createMultilingualMTextType(String txt) {
        LocalisedMTextType lTxt = createLocalizedMText(txt);
        if(lTxt==null) {
            return null;
        }
        MultilingualMTextType ret=new MultilingualMTextType();
        ret.getLocalisedText().add(lTxt);
        return ret;
    }
    private MultilingualTextType createMultilingualTextType(String txt) {
        LocalisedTextType lTxt = createLocalizedText(txt);
        if(lTxt==null) {
            return null;
        }
        MultilingualTextType ret=new MultilingualTextType();
        ret.getLocalisedText().add(lTxt);
        return ret;
    }
    private MultilingualUriType createMultilingualUri_de(String txt) {
        LocalisedUriType lTxt = createLocalizedUri_de(txt);
        if(lTxt==null) {
            return null;
        }
        MultilingualUriType ret=new MultilingualUriType();
        ret.getLocalisedText().add(lTxt);
        return ret;
    }

    private LocalisedMTextType createLocalizedMText(String txt) {
        if(txt==null || txt.length()==0) {
            return null;
        }
        LocalisedMTextType lTxt= new LocalisedMTextType();
        lTxt.setLanguage(DE);
        lTxt.setText(txt);
        return lTxt;
    }
    private LocalisedTextType createLocalizedText(String txt) {
        if(txt==null || txt.length()==0) {
            return null;
        }
        LocalisedTextType lTxt= new LocalisedTextType();
        lTxt.setLanguage(DE);
        lTxt.setText(txt);
        return lTxt;
    }
    private LocalisedUriType createLocalizedUri_de(String txt) {
        if(txt==null || txt.length()==0) {
            return null;
        }
        LocalisedUriType lTxt= new LocalisedUriType();
        lTxt.setLanguage(DE);
        lTxt.setText(txt);
        return lTxt;
    }
    private WebReferenceType createWebReferenceType(String url) {
        if(url==null || url.trim().length()==0) {
            return null;
        }
        WebReferenceType ret=new WebReferenceType();
        ret.setValue(url);
        return ret;
    }

    private String getSchema() {
        return dbschema!=null?dbschema:"xoereb";
    }
    
    private Coordinate parseCoord(String xy) {
        int sepPos=xy.indexOf(',');
        double x=Double.parseDouble(xy.substring(0, sepPos));
        double y=Double.parseDouble(xy.substring(sepPos+1));
        Coordinate coord=new Coordinate(x,y);
        return coord;
    }
    private Grundstueck getParcelByEgrid(String egrid) {
        PrecisionModel precisionModel=new PrecisionModel(1000.0);
        GeometryFactory geomFactory=new GeometryFactory(precisionModel);
        List<Grundstueck> gslist=jdbcTemplate.query(
                "SELECT ST_AsBinary(l.geometrie) as l_geometrie,ST_AsBinary(s.geometrie) as s_geometrie,ST_AsBinary(b.geometrie) as b_geometrie,nummer,nbident,art,gesamteflaechenmass,l.flaechenmass as l_flaechenmass,s.flaechenmass as s_flaechenmass,b.flaechenmass as b_flaechenmass FROM "+getSchema()+"."+TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_GRUNDSTUECK+" g"
                        +" LEFT JOIN "+getSchema()+"."+TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_LIEGENSCHAFT+" l ON g.t_id=l.liegenschaft_von "
                        +" LEFT JOIN "+getSchema()+"."+TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_SELBSTRECHT+" s ON g.t_id=s.selbstrecht_von"
                        +" LEFT JOIN "+getSchema()+"."+TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_BERGWERK+" b ON g.t_id=b.bergwerk_von"
                        +" WHERE g.egris_egrid=?", new RowMapper<Grundstueck>() {
                    WKBReader decoder=new WKBReader(geomFactory);
                    
                    @Override
                    public Grundstueck mapRow(ResultSet rs, int rowNum) throws SQLException {
                        Geometry polygon=null;
                        byte l_geometrie[]=rs.getBytes("l_geometrie");
                        byte s_geometrie[]=rs.getBytes("s_geometrie");
                        byte b_geometrie[]=rs.getBytes("b_geometrie");
                        try {
                            if(l_geometrie!=null) {
                                polygon=decoder.read(l_geometrie);
                            }else if(s_geometrie!=null) {
                                polygon=decoder.read(s_geometrie);
                            }else if(b_geometrie!=null) {
                                polygon=decoder.read(b_geometrie);
                            }else {
                                throw new IllegalStateException("no geometrie");
                            }
                            if(polygon==null || polygon.isEmpty()) {
                                return null;
                            }
                        } catch (ParseException e) {
                            throw new IllegalStateException(e);
                        }
                        Grundstueck ret=new Grundstueck();
                        ret.setGeometrie(polygon);
                        ret.setEgrid(egrid);
                        ret.setNummer(rs.getString("nummer"));
                        ret.setNbident(rs.getString("nbident"));
                        ret.setArt(rs.getString("art"));
                        int f=rs.getInt("gesamteflaechenmass");
                        if(rs.wasNull()) {
                            if(l_geometrie!=null) {
                                f=rs.getInt("l_flaechenmass");
                            }else if(s_geometrie!=null) {
                                f=rs.getInt("s_flaechenmass");
                            }else if(b_geometrie!=null) {
                                f=rs.getInt("b_flaechenmass");
                            }else {
                                throw new IllegalStateException("no geometrie");
                            }
                        }
                        ret.setFlaechenmas(f);
                        return ret;
                    }

                    
                },egrid);
        if(gslist==null || gslist.isEmpty()) {
            return null;
        }
        Polygon polygons[]=new Polygon[gslist.size()];
        int i=0;
        for(Grundstueck gs:gslist) {
            polygons[i++]=(Polygon)gs.getGeometrie();
        }
        Geometry multiPolygon=geomFactory.createMultiPolygon(polygons);
        Grundstueck gs=gslist.get(0);
        gs.setGeometrie(multiPolygon);
        
        // grundbuchkreis
        try {
            java.util.Map<String,Object> gbKreis=jdbcTemplate.queryForMap(
                    "SELECT aname,gemeinde FROM "+getSchema()+"."+OERBKRMVS_V2_0KONFIGURATION_GRUNDBUCHKREIS+" WHERE nbident=?",gs.getNbident());
            gs.setGbSubKreis((String)gbKreis.get("aname"));
            gs.setBfsNr((Integer)gbKreis.get("gemeinde"));
        }catch(EmptyResultDataAccessException ex) {
            logger.warn("no gbkreis-name for nbident {}",gs.getNbident());
        }
        
        return gs;
    }
    private Grundstueck getParcelByNumber(String nbident,String nr) {
        PrecisionModel precisionModel=new PrecisionModel(1000.0);
        GeometryFactory geomFactory=new GeometryFactory(precisionModel);
        List<Grundstueck> gslist=jdbcTemplate.query(
                "SELECT"
                + " ST_AsBinary(l.geometrie) as l_geometrie"
                + ",ST_AsBinary(s.geometrie) as s_geometrie"
                + ",ST_AsBinary(b.geometrie) as b_geometrie"
                + ",egris_egrid"
                + ",art"
                + ",gesamteflaechenmass"
                + ",l.flaechenmass as l_flaechenmass"
                + ",s.flaechenmass as s_flaechenmass"
                + ",b.flaechenmass as b_flaechenmass"
                + " FROM "+getSchema()+"."+TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_GRUNDSTUECK+" g"
                        +" LEFT JOIN "+getSchema()+"."+TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_LIEGENSCHAFT+" l ON g.t_id=l.liegenschaft_von "
                        +" LEFT JOIN "+getSchema()+"."+TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_SELBSTRECHT+" s ON g.t_id=s.selbstrecht_von"
                        +" LEFT JOIN "+getSchema()+"."+TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_BERGWERK+" b ON g.t_id=b.bergwerk_von"
                        +" WHERE g.nbident=? AND g.nummer=?", new RowMapper<Grundstueck>() {
                    WKBReader decoder=new WKBReader(geomFactory);
                    
                    @Override
                    public Grundstueck mapRow(ResultSet rs, int rowNum) throws SQLException {
                        Geometry polygon=null;
                        byte l_geometrie[]=rs.getBytes("l_geometrie");
                        byte s_geometrie[]=rs.getBytes("s_geometrie");
                        byte b_geometrie[]=rs.getBytes("b_geometrie");
                        try {
                            if(l_geometrie!=null) {
                                polygon=decoder.read(l_geometrie);
                            }else if(s_geometrie!=null) {
                                polygon=decoder.read(s_geometrie);
                            }else if(b_geometrie!=null) {
                                polygon=decoder.read(b_geometrie);
                            }else {
                                throw new IllegalStateException("no geometrie");
                            }
                            if(polygon==null || polygon.isEmpty()) {
                                return null;
                            }
                        } catch (ParseException e) {
                            throw new IllegalStateException(e);
                        }
                        Grundstueck ret=new Grundstueck();
                        ret.setGeometrie(polygon);
                        ret.setEgrid(rs.getString("egris_egrid"));
                        ret.setNummer(nr);
                        ret.setNbident(nbident);
                        ret.setArt(rs.getString("art"));
                        int f=rs.getInt("gesamteflaechenmass");
                        if(rs.wasNull()) {
                            if(l_geometrie!=null) {
                                f=rs.getInt("l_flaechenmass");
                            }else if(s_geometrie!=null) {
                                f=rs.getInt("s_flaechenmass");
                            }else if(b_geometrie!=null) {
                                f=rs.getInt("b_flaechenmass");
                            }else {
                                throw new IllegalStateException("no geometrie");
                            }
                        }
                        ret.setFlaechenmas(f);
                        return ret;
                    }

                    
                },nbident,nr);
        if(gslist==null || gslist.isEmpty()) {
            return null;
        }
        Polygon polygons[]=new Polygon[gslist.size()];
        int i=0;
        for(Grundstueck gs:gslist) {
            polygons[i++]=(Polygon)gs.getGeometrie();
        }
        Geometry multiPolygon=geomFactory.createMultiPolygon(polygons);
        Grundstueck gs=gslist.get(0);
        gs.setGeometrie(multiPolygon);
        
        // grundbuchkreis
        try {
            
            java.util.Map<String,Object> gbKreis=jdbcTemplate.queryForMap(
                    "SELECT aname,gemeinde FROM "+getSchema()+"."+OERBKRMVS_V2_0KONFIGURATION_GRUNDBUCHKREIS+" WHERE nbident=?",gs.getNbident());
            gs.setGbSubKreis((String)gbKreis.get("aname"));
            gs.setBfsNr((Integer)gbKreis.get("gemeinde"));
        }catch(EmptyResultDataAccessException ex) {
            logger.warn("no gbkreis for nbident {}",gs.getNbident());
        }
        
        return gs;
    }
    private Geometry getParcelGeometryByEgrid(String egrid) {
            PrecisionModel precisionModel=new PrecisionModel(1000.0);
            GeometryFactory geomFactory=new GeometryFactory(precisionModel);
            List<Geometry> gslist=jdbcTemplate.query(
                    "SELECT ST_AsBinary(l.geometrie) as l_geometrie,ST_AsBinary(s.geometrie) as s_geometrie,ST_AsBinary(b.geometrie) as b_geometrie FROM "+getSchema()+"."+TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_GRUNDSTUECK+" g"
                            +" LEFT JOIN "+getSchema()+"."+TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_LIEGENSCHAFT+" l ON g.t_id=l.liegenschaft_von "
                            +" LEFT JOIN "+getSchema()+"."+TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_SELBSTRECHT+" s ON g.t_id=s.selbstrecht_von"
                            +" LEFT JOIN "+getSchema()+"."+TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_BERGWERK+" b ON g.t_id=b.bergwerk_von"
                            +" WHERE g.egris_egrid=?", new RowMapper<Geometry>() {
                        WKBReader decoder=new WKBReader(geomFactory);
                        
                        @Override
                        public Geometry mapRow(ResultSet rs, int rowNum) throws SQLException {
                            Geometry polygon=null;
                            byte l_geometrie[]=rs.getBytes("l_geometrie");
                            byte s_geometrie[]=rs.getBytes("s_geometrie");
                            byte b_geometrie[]=rs.getBytes("b_geometrie");
                            try {
                                if(l_geometrie!=null) {
                                    polygon=decoder.read(l_geometrie);
                                }else if(s_geometrie!=null) {
                                    polygon=decoder.read(s_geometrie);
                                }else if(b_geometrie!=null) {
                                    polygon=decoder.read(b_geometrie);
                                }else {
                                    throw new IllegalStateException("no geometrie");
                                }
                                if(polygon==null || polygon.isEmpty()) {
                                    return null;
                                }
                            } catch (ParseException e) {
                                throw new IllegalStateException(e);
                            }
                            return polygon;
                        }

                        
                    },egrid);
            if(gslist==null || gslist.isEmpty()) {
                return null;
            }
            Geometry multiPolygon=geomFactory.createMultiPolygon(gslist.toArray(new Polygon[gslist.size()]));
            return multiPolygon;
    }
    public void setThemes(final List<ThemeType> themes, List<TopicCode> topicCodes) {
        for(TopicCode topicCode:topicCodes) {
            ThemeType themeEle1=new ThemeType();
            themeEle1.setCode(topicCode.getMainCode());
            themeEle1.setSubCode(topicCode.getSubCode());
            themeEle1.setText(getTopicText(topicCode));
            ThemeType themeEle = themeEle1;
            themes.add(themeEle);
        }
    }

    private String getQualifiedThemeCode(String themeCode,String subCode,String otherCode) {
        String qualifiedThemeCode=null;
        if(subCode==null && otherCode==null) {
            qualifiedThemeCode=themeCode;
        }else if(otherCode!=null) {
            qualifiedThemeCode=otherCode;
        }else{
            qualifiedThemeCode=subCode;
        }
        return qualifiedThemeCode;
    }

    private List<RestrictionOnLandownershipType> getRestrictions(Geometry parcelGeom,Envelope bbox,boolean withGeometry, boolean withImages,int dpi,
            List<TopicCode> queryTopics, List<TopicCode> concernedTopicsList) {
        // select schnitt parcelGeom/oerebGeom where restritctionTopic in queryTopic
        WKBWriter geomEncoder=new WKBWriter(2,ByteOrderValues.BIG_ENDIAN);
        PrecisionModel precisionModel=new PrecisionModel(1000.0);
        GeometryFactory geomFactory=new GeometryFactory(precisionModel);
        byte filterGeom[]=geomEncoder.write(geomFactory.toGeometry(bbox));
        WKBReader geomDecoder=new WKBReader(geomFactory);
        double parcelArea=parcelGeom.getArea();
        
        String sqlStmt="SELECT " + 
        "g.t_id as g_id," + 
        "ea.aname_de as ea_aname_de," + 
        "ea_lu.atext as ea_amtimweb," + 
        "ea_lu_de.atext as ea_amtimweb_de," + 
        "ea.auid as ea_auid," + 
        "d.t_id as d_id," + 
        "d_lu.atext as verweiswms," + 
        "d_lu_de.atext as verweiswms_de," + 
        "e.t_id as e_id," + 
        "leg.t_id as l_id," + 
        "leg.darstellungsdienst as l_d_id," + 
        "leg.legendetext_de," + 
        "leg.thema," + 
        "leg.subthema," + 
        "leg.artcode," + 
        "leg.artcodeliste," + 
        (withImages?" leg.symbol,":"") + 
        "e.rechtsstatus as e_rechtsstatus," + 
        "e.publiziertab as e_publiziertab," + 
        "e.publiziertbis as e_publiziertbis," + 
        "g.rechtsstatus as g_rechtsstatus," + 
        "g.publiziertab as g_publiziertab," + 
        "g.publiziertbis as g_publiziertbis," + 
        "ST_AsBinary(g.punkt) as punkt," + 
        "ST_AsBinary(g.linie) as linie," + 
        "ST_AsBinary(g.flaeche) as flaeche," + 
        "g.metadatengeobasisdaten" + 
        " FROM "+getSchema()+"."+OERBKRMFR_V2_0TRANSFERSTRUKTUR_GEOMETRIE+" as g " + 
        " INNER JOIN "+getSchema()+"."+OERBKRMFR_V2_0TRANSFERSTRUKTUR_EIGENTUMSBESCHRAENKUNG+" as e ON g.eigentumsbeschraenkung = e.t_id" + 
        " INNER JOIN "+getSchema()+"."+OERBKRMFR_V2_0TRANSFERSTRUKTUR_DARSTELLUNGSDIENST+" as d ON e.darstellungsdienst = d.t_id" + 
        " INNER JOIN "+getSchema()+"."+OEREBKRM_V2_0AMT_AMT+" as ea ON e.zustaendigestelle = ea.t_id"+
        " INNER JOIN "+getSchema()+"."+OERBKRMFR_V2_0TRANSFERSTRUKTUR_LEGENDEEINTRAG+" as leg ON e.legende = leg.t_id"+
        " LEFT JOIN "+getSchema()+"."+OEREBKRM_V2_0_MULTILINGUALURI+" as ea_mu ON ea.t_id = ea_mu.oerebkrm_v2_0amt_amt_amtimweb"+" LEFT JOIN (SELECT atext,oerbkrm_v2__mltlngluri_localisedtext FROM "+getSchema()+"."+OEREBKRM_V2_0_LOCALISEDURI+" WHERE alanguage IS NULL) as ea_lu ON ea_mu.t_id = ea_lu.oerbkrm_v2__mltlngluri_localisedtext"+
        " LEFT JOIN "+getSchema()+"."+OEREBKRM_V2_0_MULTILINGUALURI+" as ea_mu_de ON ea.t_id = ea_mu_de.oerebkrm_v2_0amt_amt_amtimweb"+" LEFT JOIN (SELECT atext,oerbkrm_v2__mltlngluri_localisedtext FROM "+getSchema()+"."+OEREBKRM_V2_0_LOCALISEDURI+" WHERE alanguage='de') as ea_lu_de ON ea_mu_de.t_id = ea_lu_de.oerbkrm_v2__mltlngluri_localisedtext"+
        " LEFT JOIN "+getSchema()+"."+OEREBKRM_V2_0_MULTILINGUALURI+" as d_mu ON d.t_id = d_mu.oerbkrmfr_vstllngsdnst_verweiswms AND d_mu.oerbkrmfr_vstllngsdnst_verweiswms IS NOT NULL"+" LEFT JOIN (SELECT atext,oerbkrm_v2__mltlngluri_localisedtext FROM "+getSchema()+"."+OEREBKRM_V2_0_LOCALISEDURI+" WHERE alanguage IS NULL) as d_lu ON d_mu.t_id = d_lu.oerbkrm_v2__mltlngluri_localisedtext"+
        " LEFT JOIN "+getSchema()+"."+OEREBKRM_V2_0_MULTILINGUALURI+" as d_mu_de ON d.t_id = d_mu_de.oerbkrmfr_vstllngsdnst_verweiswms AND d_mu_de.oerbkrmfr_vstllngsdnst_verweiswms IS NOT NULL"+" LEFT JOIN (SELECT atext,oerbkrm_v2__mltlngluri_localisedtext FROM "+getSchema()+"."+OEREBKRM_V2_0_LOCALISEDURI+" WHERE alanguage='de') as d_lu_de ON d_mu_de.t_id = d_lu_de.oerbkrm_v2__mltlngluri_localisedtext"+
        //" INNER JOIN "+getSchema()+"."+TABLE_OERBKRMVS_V1_1VORSCHRIFTEN_AMT+" as ga ON g.zustaendigestelle = ga.t_id"+
        " WHERE (ST_DWithin(ST_GeomFromWKB(:geom,2056),flaeche,0.1) OR ST_DWithin(ST_GeomFromWKB(:geom,2056),linie,0.1) OR ST_DWithin(ST_GeomFromWKB(:geom,2056),punkt,0.1)) ";
        Set<TopicCode> concernedTopics=new HashSet<TopicCode>();
        Map<Long,TopicCode> restriction2topicCode=new HashMap<Long,TopicCode>();
        Map<Long,RestrictionOnLandownershipType> restrictions=new HashMap<Long,RestrictionOnLandownershipType>();
        Map<Long,Integer> restrictionsPointCount=new HashMap<Long,Integer>();
        Map<Long,Double> restrictionsLengthShare=new HashMap<Long,Double>();
        Map<Long,Double> restrictionsAreaShare=new HashMap<Long,Double>();
        Map<Long,Long> restriction2mapid=new HashMap<Long,Long>();
        Map<Long,Long> restriction2legendeid=new HashMap<Long,Long>();
        Set<Long> concernedRestrictions=new HashSet<Long>();
        Map<Long,Map<Long,LegendEntryType>> legendPerWms=new HashMap<Long,Map<Long,LegendEntryType>>();
        Map<Long,Set<Long>> otherLegendCodesPerMap=new HashMap<Long,Set<Long>>();
        ArrayList<String> queryTopicCodes = new ArrayList<String>();
        ArrayList<String> querySubTopicCodes = new ArrayList<String>();
        for(TopicCode topicCode:queryTopics) {
            if(topicCode.isSubTopic()) {
                querySubTopicCodes.add(topicCode.getSubCode());
            }else {
                queryTopicCodes.add(topicCode.getMainCode());
            }
        }
        MapSqlParameterSource parameters = new MapSqlParameterSource();
        parameters.addValue("topics", queryTopicCodes);
        if(!querySubTopicCodes.isEmpty()) {
            parameters.addValue("subtopics", querySubTopicCodes);
            sqlStmt=sqlStmt+ "AND (thema in (:topics) OR subthema in (:subtopics))";
        }else {
            sqlStmt=sqlStmt+ "AND (thema in (:topics))";
            
        }
        parameters.addValue("geom", filterGeom);
        logger.info("stmt {} ",sqlStmt);
        jdbcParamTemplate.query(sqlStmt, parameters,new ResultSetExtractor<Object>() {

            @Override
            public Object extractData(ResultSet rs) throws SQLException, DataAccessException {
                while(rs.next()) {
                    long g_id=rs.getLong("g_id");
                    long e_id=rs.getLong("e_id");
                    long d_id=rs.getLong("d_id");
                    long l_id=rs.getLong("l_id");
                    long l_d_id=rs.getLong("l_d_id");
                    final String aussage_de = rs.getString("legendetext_de");
                    logger.info("g_id {} e_id {} d_id {} l_id {} l_d_id {} aussage {} ",g_id,e_id,d_id,l_id,l_d_id,aussage_de);
                    if(d_id!=l_d_id) {
                        throw new IllegalArgumentException("LegendeEintrag "+l_id+" passt nicht zu Darstellungsdienst "+d_id);
                    }
                    java.util.Date today=new java.util.Date();
                    java.sql.Date e_publiziertab = rs.getDate("e_publiziertab");
                    java.sql.Date e_publiziertbis = rs.getDate("e_publiziertbis");
                    java.sql.Date g_publiziertab = rs.getDate("g_publiziertab");
                    java.sql.Date g_publiziertbis = rs.getDate("g_publiziertbis");
                    if(today.before(e_publiziertab)) {
                        continue;
                    }
                    if(e_publiziertbis!=null && today.after(e_publiziertbis)) {
                        continue;
                    }
                    if(today.before(g_publiziertab)) {
                        continue;
                    }
                    if(g_publiziertbis!=null && today.after(g_publiziertbis)) {
                        continue;
                    }
                    RestrictionOnLandownershipType rest=restrictions.get(e_id);
                    if(rest==null) {
                        
                        RestrictionOnLandownershipType localRest=new RestrictionOnLandownershipType();
                        rest=localRest;
                        restrictions.put(e_id,rest);
                        restriction2mapid.put(e_id,d_id);
                        restriction2legendeid.put(e_id,l_id);
                        
                        rest.setLegendText(createMultilingualMTextType(aussage_de));
                        rest.setLawstatus(mapLawstatus(rs.getString("e_rechtsstatus")));
                        String topic=rs.getString("thema");
                        String subThema=rs.getString("subthema"); 
                        TopicCode qtopic=new TopicCode(topic,subThema);
                        restriction2topicCode.put(e_id, qtopic);
                        ThemeType themeEle1=new ThemeType();
                        themeEle1.setCode(topic);
                        themeEle1.setText(getTopicText(qtopic));
                        themeEle1.setSubCode(subThema);
                        ThemeType themeEle = themeEle1;
                        rest.setTheme(themeEle);
                        String typeCode=rs.getString("artcode"); 
                        String typeCodelist=rs.getString("artcodeliste"); 
                        rest.setTypeCode(typeCode);
                        rest.setTypeCodelist(typeCodelist);
                        
                        OfficeType zustaendigeStelle=new OfficeType();
                        String ea_name=rs.getString("ea_aname_de");
                        zustaendigeStelle.setName(createMultilingualTextType(ea_name));
                        String ea_amtimweb=rs.getString("ea_amtimweb_de");
                        if(ea_amtimweb==null) {
                            ea_amtimweb=rs.getString("ea_amtimweb");
                        }
                        zustaendigeStelle.setOfficeAtWeb(createMultilingualUri(ea_amtimweb));
                        zustaendigeStelle.setUID(rs.getString("ea_auid"));
                        rest.setResponsibleOffice(zustaendigeStelle);
                        
                        MapType map=new MapType();
                        String wmsUrl=rs.getString("verweiswms_de");
                        if(wmsUrl==null) {
                            wmsUrl=rs.getString("verweiswms");
                        }
                        wmsUrl = getWmsUrl(bbox, wmsUrl,dpi);
                        map.setReferenceWMS(createMultilingualUri(wmsUrl));
                        if(withImages) {
                            try {
                                byte wmsImage[]=getWmsImage(wmsUrl);
                                map.setImage(createMultilingualBlob(wmsImage));
                            } catch (IOException | URISyntaxException e) {
                                logger.error("failed to get wms image",e);
                                map.setImage(createMultilingualBlob(minimalImage));
                            }
                        }
                        double layerOpacity[]=new double[1];
                        Integer layerIndex=getLayerIndex(wmsUrl,layerOpacity);
                        if(layerIndex==null) {
                            layerIndex=0;
                            layerOpacity[0]=0.6;
                        }
                        map.setLayerIndex(layerIndex);
                        map.setLayerOpacity(layerOpacity[0]);
                        if(withGeometry) {
                            setMapBBOX(map,bbox);
                        }
                        
                        Map<Long,LegendEntryType> legendEntries=legendPerWms.get(d_id);
                        // WMS not yet seen?
                        if(legendEntries==null){
                            otherLegendCodesPerMap.put(d_id, new HashSet<Long>());
                            // collect legend entries
                            Map<Long,LegendEntryType> localLegendEntries=new HashMap<Long,LegendEntryType>();
                            legendEntries=localLegendEntries;
                            legendPerWms.put(d_id,legendEntries);
                            String stmt="SELECT" + 
                                    "  t_id" + 
                                    (withImages?" ,symbol":"") + 
                                    "  ,legendetext_de" + 
                                    "  ,artcode" + 
                                    "  ,artcodeliste" + 
                                    "  ,thema" + 
                                    "  ,subthema" + 
                                    "  " + 
                                    "FROM "+getSchema()+"."+OERBKRMFR_V2_0TRANSFERSTRUKTUR_LEGENDEEINTRAG+" WHERE darstellungsdienst=?";
                            logger.info("stmt {} ",stmt);
                            jdbcTemplate.query(stmt, new RowCallbackHandler() {

                                @Override
                                public void processRow(ResultSet rs) throws SQLException {
                                    long t_id=rs.getLong("t_id");
                                    final String l_code = rs.getString("artcode");
                                    final String l_codelist = rs.getString("artcodeliste");
                                    LegendEntryType l=new LegendEntryType();
                                    l.setLegendText(createMultilingualTextType(rs.getString("legendetext_de")));
                                    String legendTopic=rs.getString("thema");
                                    TopicCode qualifiedThemeCode=new TopicCode(legendTopic,rs.getString("subthema"));
                                    ThemeType themeEle=new ThemeType();
                                    themeEle.setCode(legendTopic);
                                    themeEle.setSubCode(rs.getString("subthema"));
                                    themeEle.setText(getTopicText(qualifiedThemeCode));
                                    ThemeType legendThemeEle = themeEle;
                                    l.setTheme(legendThemeEle);
                                    if(withImages) {
                                        l.setSymbol(rs.getBytes("symbol"));
                                    }else {
                                        l.setSymbolRef(getSymbolRef(Long.toString(t_id)));
                                    }
                                    l.setTypeCode(l_code);
                                    l.setTypeCodelist(l_codelist);
                                    localLegendEntries.put(t_id,l);
                                }
                            },d_id);
                        }
                        if(withImages) {
                            rest.setSymbol(rs.getBytes("symbol"));
                        }else {
                            rest.setSymbolRef(getSymbolRef(Long.toString(l_id)));
                        }
                        rest.setMap(map);
                        
                        // Dokumente
                        Map<Long,DocumentType> documentMap = new HashMap<Long,DocumentType>();
                        Map<Long,Long> documentOrdering = new HashMap<Long,Long>();

                        // Rechtsvorschriften
                        {
                            String stmt= 
                                    "select "
                                    +"ed.t_id"
                                    +",ed.typ"
                                    +",ed.titel_de"
                                    +",ed.abkuerzung_de"
                                    +",ed.offiziellenr_de"
                                    +",ed.auszugindex"
                                    +",ed.publiziertab as d_publiziertab"
                                    +",ed.publiziertbis as d_publiziertbis"
                                    +",docuri1.docuri"
                                    +",ea.aname_de as a_aname_de" 
                                    +",NULL as a_amtimweb" // ",ea.amtimweb as a_amtimweb" 
                                    +",ea.auid as a_auid"
                                    +",ed.rechtsstatus"
                                    
                                    + " from "+getSchema()+"."+OERBKRMFR_V2_0TRANSFERSTRUKTUR_HINWEISVORSCHRIFT+" as h "
                                    + "      INNER JOIN "+getSchema()+"."+OEREBKRM_V2_0DOKUMENTE_DOKUMENT+" as ed on h.vorschrift=ed.t_id"
                                    + "      INNER JOIN (SELECT "+OEREBKRM_V2_0_MULTILINGUALURI+".oerbkrm_v2_kmnt_dkment_textimweb as docid,"+OEREBKRM_V2_0_LOCALISEDURI+".atext as docuri FROM  "+getSchema()+"."+OEREBKRM_V2_0_MULTILINGUALURI+" INNER JOIN "+getSchema()+"."+OEREBKRM_V2_0_LOCALISEDURI+" ON  "+OEREBKRM_V2_0_LOCALISEDURI+".oerbkrm_v2__mltlngluri_localisedtext = "+OEREBKRM_V2_0_MULTILINGUALURI+".t_id WHERE alanguage='de') as docuri1 ON docuri1.docid=ed.t_id"
                                    + "      INNER JOIN "+getSchema()+"."+OEREBKRM_V2_0AMT_AMT+" as ea ON ed.zustaendigestelle = ea.t_id"
                                    +"  where eigentumsbeschraenkung=?"
                                    ;
                            logger.info("stmt {} ",stmt);

                            jdbcTemplate.query(stmt, new RowCallbackHandler() {

                                @Override
                                public void processRow(ResultSet rs) throws SQLException {
                                    DocumentType doc=new DocumentType();
                                    long docid=rs.getLong("t_id");
                                    long docidx=rs.getLong("auszugindex");
                                    java.sql.Date d_publiziertab = rs.getDate("d_publiziertab");
                                    java.sql.Date d_publiziertbis = rs.getDate("d_publiziertbis");
                                    if(today.before(d_publiziertab)) {
                                        return;
                                    }
                                    if(d_publiziertbis!=null && today.after(d_publiziertbis)) {
                                        return;
                                    }
                                    doc.setType(mapDocumentType(rs.getString("typ")));
                                    doc.setLawstatus(mapLawstatus(rs.getString("rechtsstatus")));
                                    doc.setTitle(createMultilingualTextType(rs.getString("titel_de")));
                                    doc.setAbbreviation(createMultilingualTextType(rs.getString("abkuerzung_de")));
                                    doc.setOfficialNumber(createMultilingualTextType(rs.getString("offiziellenr_de")));
                                    doc.setTextAtWeb(createMultilingualUri_de(rs.getString("docuri")));
                                    doc.setIndex((int)docidx);
                                    OfficeType zustaendigeStelle=new OfficeType();
                                    zustaendigeStelle.setName(createMultilingualTextType(rs.getString("a_aname_de")));
                                    zustaendigeStelle.setOfficeAtWeb(createMultilingualUri(rs.getString("a_amtimweb")));
                                    zustaendigeStelle.setUID(rs.getString("a_auid"));
                                    doc.setResponsibleOffice(zustaendigeStelle);
                                    
                                    documentMap.put(docid,doc);
                                    documentOrdering.put(docid,docidx);
                                }
                            },e_id);
                        }
                        // Gesetze
                        {
                            TopicCode topicCode=restriction2topicCode.get(e_id);
                            MapSqlParameterSource parameters = new MapSqlParameterSource();
                            String stmt= 
                                    "select "
                                    +"ed.t_id"
                                    +",ed.typ"
                                    +",ed.titel_de"
                                    +",ed.abkuerzung_de"
                                    +",ed.offiziellenr_de"
                                    +",ed.auszugindex"
                                    +",ed.publiziertab as d_publiziertab"
                                    +",ed.publiziertbis as d_publiziertbis"
                                    +",docuri1.docuri"
                                    +",ea.aname_de as a_aname_de" 
                                    +",NULL as a_amtimweb" // ",ea.amtimweb as a_amtimweb" 
                                    +",ea.auid as a_auid"
                                    +",ed.rechtsstatus"
                                    +" from "+getSchema()+"."+OERBKRMVS_V2_0THEMA_THEMA+" as t"
                                    + " INNER JOIN "+getSchema()+"."+OERBKRMVS_V2_0THEMA_THEMAGESETZ+" as tg ON t.t_id=tg.thema "
                                    + "      INNER JOIN "+getSchema()+"."+OEREBKRM_V2_0DOKUMENTE_DOKUMENT+" as ed on tg.gesetz=ed.t_id"
                                    + "      INNER JOIN (SELECT "+OEREBKRM_V2_0_MULTILINGUALURI+".oerbkrm_v2_kmnt_dkment_textimweb as docid,"+OEREBKRM_V2_0_LOCALISEDURI+".atext as docuri FROM  "+getSchema()+"."+OEREBKRM_V2_0_MULTILINGUALURI+" INNER JOIN "+getSchema()+"."+OEREBKRM_V2_0_LOCALISEDURI+" ON  "+OEREBKRM_V2_0_LOCALISEDURI+".oerbkrm_v2__mltlngluri_localisedtext = "+OEREBKRM_V2_0_MULTILINGUALURI+".t_id WHERE alanguage='de') as docuri1 ON docuri1.docid=ed.t_id"
                                    + "      INNER JOIN "+getSchema()+"."+OEREBKRM_V2_0AMT_AMT+" as ea ON ed.zustaendigestelle = ea.t_id";
                            if(topicCode.isSubTopic()) {
                                parameters.addValue("topics", topicCode.getMainCode());
                                parameters.addValue("subtopics", topicCode.getSubCode());
                                stmt=stmt+"  WHERE (t.acode IN (:topics) AND t.subcode IS NULL) OR t.subcode IN (:subtopics)";
                            }else {
                                parameters.addValue("topics", topicCode.getMainCode());
                                stmt=stmt+"  WHERE (t.acode IN (:topics) AND t.subcode IS NULL)";
                            }
                            logger.info("stmt {} ",stmt);

                            jdbcParamTemplate.query(stmt, parameters,new RowCallbackHandler() {

                                @Override
                                public void processRow(ResultSet rs) throws SQLException {
                                    DocumentType doc=new DocumentType();
                                    long docid=rs.getLong("t_id");
                                    long docidx=rs.getLong("auszugindex");
                                    java.sql.Date d_publiziertab = rs.getDate("d_publiziertab");
                                    java.sql.Date d_publiziertbis = rs.getDate("d_publiziertbis");
                                    if(today.before(d_publiziertab)) {
                                        return;
                                    }
                                    if(d_publiziertbis!=null && today.after(d_publiziertbis)) {
                                        return;
                                    }
                                    doc.setType(mapDocumentType(rs.getString("typ")));
                                    doc.setLawstatus(mapLawstatus(rs.getString("rechtsstatus")));
                                    doc.setTitle(createMultilingualTextType(rs.getString("titel_de")));
                                    doc.setAbbreviation(createMultilingualTextType(rs.getString("abkuerzung_de")));
                                    doc.setOfficialNumber(createMultilingualTextType(rs.getString("offiziellenr_de")));
                                    doc.setTextAtWeb(createMultilingualUri_de(rs.getString("docuri")));
                                    doc.setIndex((int)docidx);
                                    OfficeType zustaendigeStelle=new OfficeType();
                                    zustaendigeStelle.setName(createMultilingualTextType(rs.getString("a_aname_de")));
                                    zustaendigeStelle.setOfficeAtWeb(createMultilingualUri(rs.getString("a_amtimweb")));
                                    zustaendigeStelle.setUID(rs.getString("a_auid"));
                                    doc.setResponsibleOffice(zustaendigeStelle);
                                    
                                    documentMap.put(docid,doc);
                                    documentOrdering.put(docid,docidx);
                                }
                            });
                        }
                        List<Long> docids=new ArrayList<Long>(documentOrdering.keySet());
                        docids.sort(new Comparator<Long>() {
                            @Override
                            public int compare(Long o1, Long o2) {
                                return documentOrdering.get(o1).compareTo(documentOrdering.get(o2));
                            }
                        });
                        List<DocumentType> documents = rest.getLegalProvisions();
                        for(Long docid:docids) {
                            documents.add(documentMap.get(docid));
                        }
                    }
                   
                    QualifiedCode thisCode=new QualifiedCode(rest.getTypeCodelist(),rest.getTypeCode());
                    
                    Polygon flaeche=null;
                    LineString linie=null;
                    Point punkt=null;
                    Geometry intersection=null;
                    byte flaecheWkb[]=rs.getBytes("flaeche");
                    byte linieWkb[]=rs.getBytes("linie");
                    byte punktWkb[]=rs.getBytes("punkt");
                    if(flaecheWkb!=null) {
                        try {
                            flaeche = (Polygon) geomDecoder.read(flaecheWkb);
                        } catch (ParseException e) {
                            throw new IllegalStateException(e);
                        }
                        intersection=parcelGeom.intersection(flaeche);
                        if(!intersection.isEmpty() && intersection.getArea()<minIntersection) {
                            intersection=geomFactory.createPolygon((Coordinate[])null);
                        }
                    }else if(linieWkb!=null) {
                        try {
                            linie = (LineString) geomDecoder.read(linieWkb);
                        } catch (ParseException e) {
                            throw new IllegalStateException(e);
                        }
                        intersection=parcelGeom.intersection(linie);
                        if(!intersection.isEmpty() &&  intersection.getLength()<minIntersection) {
                            intersection=geomFactory.createLineString((Coordinate[])null);
                        }
                    }else if(punktWkb!=null) {
                        try {
                            punkt = (Point) geomDecoder.read(punktWkb);
                        } catch (ParseException e) {
                            throw new IllegalStateException(e);
                        }
                        intersection=parcelGeom.intersection(punkt);
                    }
                    
                    Set<Long> otherLegendCodes=otherLegendCodesPerMap.get(d_id);
                    if(intersection.isEmpty()) {
                        otherLegendCodes.add(l_id);
                        logger.debug("otherLegend {}",thisCode);
                    }else {
                        logger.debug("concernedCode {}",thisCode);
                        concernedRestrictions.add(e_id);
                        
                        TopicCode qtopic=restriction2topicCode.get(e_id);
                        if(!concernedTopics.contains(qtopic)) {
                            concernedTopics.add(qtopic);
                        }
                        
                        GeometryType rGeom=new GeometryType();
                        if(flaeche!=null) {
                            double area=intersection.getArea();
                            Double areaSum=restrictionsAreaShare.get(e_id);
                            if(areaSum==null) {
                                areaSum=area;
                            }else {
                                areaSum=areaSum+area;
                            }
                            restrictionsAreaShare.put(e_id,areaSum);
                            if(withGeometry) {
                                SurfaceType flaecheGml=jts2xtf.createSurfaceType(flaeche);
                                rGeom.setSurface(flaecheGml);
                            }
                        }else if(linie!=null) {
                            double length=intersection.getLength();
                            Double lengthSum=restrictionsLengthShare.get(e_id);
                            if(lengthSum==null) {
                                lengthSum=length;
                            }else {
                                lengthSum=lengthSum+length;
                            }
                            restrictionsLengthShare.put(e_id,lengthSum);
                            if(withGeometry) {
                                PolylineType linieGml=jts2xtf.createPolylineType(linie);
                                rGeom.setLine(linieGml);
                            }
                        }else if(punkt!=null) {
                            Integer pointSum=restrictionsPointCount.get(e_id);
                            if(pointSum==null) {
                                pointSum=1;
                            }else {
                                pointSum=pointSum+1;
                            }
                            restrictionsPointCount.put(e_id,pointSum);
                            if(withGeometry) {
                                CoordType pointGml=jts2xtf.createCoordType(punkt.getCoordinate());
                                rGeom.setPoint(pointGml);
                            }
                        }else {
                            throw new IllegalStateException("no geometry");
                        }
                        rGeom.setLawstatus(mapLawstatus(rs.getString("g_rechtsstatus")));
                        rGeom.setMetadataOfGeographicalBaseData(rs.getString("metadatengeobasisdaten"));
                        rest.getGeometry().add(rGeom);
                    }
                    
                }
                return null;
            }            
        }
        );
        List<RestrictionOnLandownershipType> rests=new ArrayList<RestrictionOnLandownershipType>(); 
        for(long e_id:concernedRestrictions) {
            RestrictionOnLandownershipType rest=restrictions.get(e_id);
            QualifiedCode concernedCode=new QualifiedCode(rest.getTypeCodelist(),rest.getTypeCode());
            logger.debug("e_id {} concernedCode {}",e_id,concernedCode);
            Double areaSum=restrictionsAreaShare.get(e_id);
            Double lengthSum=restrictionsLengthShare.get(e_id);
            Integer pointSum=restrictionsPointCount.get(e_id);
            if(areaSum!=null) {
                rest.setPartInPercent(new BigDecimal(Math.round(1000.0/parcelArea*areaSum)).movePointLeft(1));
                rest.setAreaShare((int)Math.round(areaSum)); 
            }else if(lengthSum!=null) {
                rest.setLengthShare((int)Math.round(lengthSum)); 
            }else if(pointSum!=null) {
                rest.setNrOfPoints(pointSum);
            }else {
                throw new IllegalStateException("no share");
            }
            
            // otherLegend ermitteln
            MapType map=rest.getMap();
            long d_id=restriction2mapid.get(e_id);
            long l_id=restriction2legendeid.get(e_id);
            Map<Long,LegendEntryType> legendEntries = legendPerWms.get(d_id);
            logger.debug("d_id {} legendEntries.size() {}",d_id,legendEntries.size());
            Set<Long> otherLegendCodes=otherLegendCodesPerMap.get(d_id);
            logger.debug("d_id {} otherLegendCodes.size() {}",d_id,otherLegendCodes.size());
            for(Long entryId:legendEntries.keySet()) {
                if(otherLegendCodes.contains(entryId) && entryId!=l_id) {
                    map.getOtherLegend().add(legendEntries.get(entryId));
                }
            }
            rests.add(rest);
        }        
        concernedTopicsList.addAll(concernedTopics);
        return rests;
    }
    protected MultilingualBlobType createMultilingualBlob(byte[] wmsImage) {
        LocalisedBlobType blob=new LocalisedBlobType();
        blob.setBlob(wmsImage);
        blob.setLanguage(LanguageCodeType.DE);
        MultilingualBlobType ret=new MultilingualBlobType();
        ret.getLocalisedBlob().add(blob);
        return ret;
    }

    protected LegendEntryType getSymbol(List<LegendEntryType> legendEntries, String typeCodelist, String typeCode) {
        for(LegendEntryType entry:legendEntries) {
            if(typeCodelist.equals(entry.getTypeCodelist()) && typeCode.equals(entry.getTypeCode())) {
                return entry;
            }
        }
        return null;
    }

    private void setMapBBOX(MapType map, Envelope bbox) {
        map.setMax(jts2xtf.createCoordType(new Coordinate(bbox.getMaxX(),bbox.getMaxY())));
        map.setMin(jts2xtf.createCoordType(new Coordinate(bbox.getMinX(),bbox.getMinY())));
    }
    private HashMap<String,LawstatusType> statusCodes=null;
    private HashMap<String,DocumentTypeType> docCodes=null;
    private HashMap<String,RealEstateTypeType> realEstateCodes=null;
    private static final int MAP_WIDTH_MM = 174;
    private static final int MAP_HEIGHT_MM = 99;
    private LawstatusType mapLawstatus(String xtfTransferCode) {
        if(statusCodes==null) {
            statusCodes=new HashMap<String,LawstatusType>();
            java.util.List<java.util.Map<String,Object>> baseData=jdbcTemplate.queryForList(
                    "SELECT acode,titel_de,titel_fr,titel_it,titel_rm,titel_en FROM "+getSchema()+"."+OERBKRMVS_V2_0KONFIGURATION_RECHTSSTATUSTXT);
            for(java.util.Map<String,Object> status:baseData) {
                MultilingualTextType statusTxt=createMultilingualTextType((String)status.get("titel_de"));
                LawstatusType lawstatus=new LawstatusType();
                lawstatus.setText(statusTxt);
                final String code = (String)status.get("acode");
                if(code.equals("inKraft")) {
                    lawstatus.setCode(LawstatusCodeType.IN_FORCE);
                }else if(code.equals("AenderungOhneVorwirkung")) {
                    lawstatus.setCode(LawstatusCodeType.CHANGE_WITHOUT_PRE_EFFECT);
                }else if(code.equals("AenderungMitVorwirkung")) {
                    lawstatus.setCode(LawstatusCodeType.CHANGE_WITH_PRE_EFFECT);
                }
                statusCodes.put(code,lawstatus);
            }
        }
        if(xtfTransferCode!=null) {
            return statusCodes.get(xtfTransferCode);
        }
        return null;
    }
    private RealEstateTypeType mapRealEstateType(String gsArt) {
        if(realEstateCodes==null) {
            realEstateCodes=new HashMap<String,RealEstateTypeType>();
            java.util.List<java.util.Map<String,Object>> baseData=jdbcTemplate.queryForList(
                    "SELECT acode,titel_de,titel_fr,titel_it,titel_rm,titel_en FROM "+getSchema()+"."+OERBKRMVS_V2_0KONFIGURATION_GRUNDSTUECKSARTTXT);
            for(java.util.Map<String,Object> status:baseData) {
                MultilingualTextType codeTxt=createMultilingualTextType((String)status.get("titel_de"));
                RealEstateTypeType gsType=new RealEstateTypeType();
                gsType.setText(codeTxt);
                final String code = (String)status.get("acode");
                if("Liegenschaft".equals(code)) {
                    gsType.setCode(RealEstateTypeCodeType.REAL_ESTATE);
                }else if("SelbstRecht.Baurecht".equals(code)) {
                    gsType.setCode(RealEstateTypeCodeType.DISTINCT_AND_PERMANENT_RIGHTS_BUILDING_RIGHT);
                }else if("SelbstRecht.Quellenrecht".equals(code)) {
                    gsType.setCode(RealEstateTypeCodeType.DISTINCT_AND_PERMANENT_RIGHTS_RIGHT_TO_SPRING_WATER);
                }else if("SelbstRecht.Konzessionsrecht".equals(code)) {
                    gsType.setCode(RealEstateTypeCodeType.DISTINCT_AND_PERMANENT_RIGHTS_CONCESSION);
                }else if("SelbstRecht.weitere".equals(code)) {
                    gsType.setCode(RealEstateTypeCodeType.DISTINCT_AND_PERMANENT_RIGHTS_OTHER);
                }else if("Bergwerk".equals(code)) {
                    gsType.setCode(RealEstateTypeCodeType.MINERAL_RIGHTS);
                }else {
                    throw new IllegalStateException("unknown code '"+code+"'");
                }
                realEstateCodes.put(code,gsType);
            }
        }
        if(gsArt!=null) {
            return realEstateCodes.get(gsArt);
        }
        return null;
    }
    private DocumentTypeType mapDocumentType(String xtfTransferCode) {
        if(docCodes==null) {
            docCodes=new HashMap<String,DocumentTypeType>();
            java.util.List<java.util.Map<String,Object>> baseData=jdbcTemplate.queryForList(
                    "SELECT acode,titel_de,titel_fr,titel_it,titel_rm,titel_en FROM "+getSchema()+"."+OERBKRMVS_V2_0KONFIGURATION_DOKUMENTTYPTXT);
            for(java.util.Map<String,Object> type:baseData) {
                MultilingualTextType typeTxt=createMultilingualTextType((String)type.get("titel_de"));
                DocumentTypeType doctyp=new DocumentTypeType();
                doctyp.setText(typeTxt);
                final String code = (String)type.get("acode");
                if(code.equals("Rechtsvorschrift")) {
                    doctyp.setCode(DocumentTypeCodeType.LEGAL_PROVISION);
                }else if(code.equals("GesetzlicheGrundlage")) {
                    doctyp.setCode(DocumentTypeCodeType.LAW);
                }else if(code.equals("Hinweis")) {
                    doctyp.setCode(DocumentTypeCodeType.HINT);
                }
                docCodes.put(code,doctyp);
            }
        }
        if(xtfTransferCode!=null) {
            return docCodes.get(xtfTransferCode);
        }
        return null;
    }

    private void setParcel(ExtractType extract, String egrid, Grundstueck parcel,Envelope bbox, boolean withGeometry,boolean withImages,int dpi) {
        WKBWriter geomEncoder=new WKBWriter(2,ByteOrderValues.BIG_ENDIAN);
        geomEncoder.write(parcel.getGeometrie());
        
        RealEstateDPRType gs = new  RealEstateDPRType();
        gs.setEGRID(egrid);
        final String nbident = parcel.getNbident();
        String canton=nbident.substring(0, 2);
        gs.setCanton(CantonCodeType.fromValue(canton));
        gs.setIdentDN(nbident);
        gs.setNumber(parcel.getNummer());
        gs.setSubunitOfLandRegister(parcel.getGbSubKreis());
        if(gs.getSubunitOfLandRegister()!=null) {
            gs.setSubunitOfLandRegisterDesignation(getSubunitDesignationOfMunicipality(parcel.getBfsNr()));
        }
        gs.setMunicipalityCode(parcel.getBfsNr());
        // gemeindename
        String gemeindename=jdbcTemplate.queryForObject(
                "SELECT aname FROM "+getSchema()+"."+TABLE_DM01VCH24LV95DGEMEINDEGRENZEN_GEMEINDE+" WHERE bfsnr=?",String.class,gs.getMunicipalityCode());
        gs.setMunicipalityName(gemeindename);
        gs.setLandRegistryArea((int)parcel.getFlaechenmas());
        String gsArt=parcel.getArt();
        gs.setType(mapRealEstateType(gsArt));
        //gs.setMetadataOfGeographicalBaseData(value);
        if(withGeometry) {
            MultiSurfaceType geomGml=jts2xtf.createMultiSurfaceType(parcel.getGeometrie());
            gs.setLimit(geomGml);
        }
        
        
        {
            // Planausschnitt 174 * 99 mm
            MapType planForLandregister=new MapType();
            String fixedWmsUrl = getWmsUrl(bbox, oerebPlanForLandregister,dpi);
            planForLandregister.setReferenceWMS(createMultilingualUri(fixedWmsUrl));
            gs.setPlanForLandRegister(planForLandregister);
            if(withImages) {
                try {
                    planForLandregister.setImage(createMultilingualBlob(getWmsImage(fixedWmsUrl)));
                } catch (IOException | URISyntaxException e) {
                    logger.error("failed to get wms image",e);
                    planForLandregister.setImage(createMultilingualBlob(minimalImage));
                }
            }
            double layerOpacity[]=new double[1];
            Integer layerIndex=getLayerIndex(oerebPlanForLandregister,layerOpacity);
            if(layerIndex==null) {
                layerIndex=0;
                layerOpacity[0]=0.6;
            }
            planForLandregister.setLayerIndex(layerIndex);
            planForLandregister.setLayerOpacity(layerOpacity[0]);
            if(withGeometry) {
                setMapBBOX(planForLandregister,bbox);
            }
        }
        {
            // Planausschnitt 174 * 99 mm
            MapType planForLandregisterMainPage=new MapType();
            String fixedWmsUrl = getWmsUrl(bbox, oerebPlanForLandregisterMainPage,dpi);
            planForLandregisterMainPage.setReferenceWMS(createMultilingualUri(fixedWmsUrl));
            gs.setPlanForLandRegisterMainPage(planForLandregisterMainPage);
            if(withImages) {
                try {
                    planForLandregisterMainPage.setImage(createMultilingualBlob(getWmsImage(fixedWmsUrl)));
                } catch (IOException | URISyntaxException e) {
                    logger.error("failed to get wms image",e);
                    planForLandregisterMainPage.setImage(createMultilingualBlob(minimalImage));
                }
            }
            double layerOpacity[]=new double[1];
            Integer layerIndex=getLayerIndex(oerebPlanForLandregisterMainPage,layerOpacity);
            if(layerIndex==null) {
                layerIndex=0;
                layerOpacity[0]=0.6;
            }
            planForLandregisterMainPage.setLayerIndex(layerIndex);
            planForLandregisterMainPage.setLayerOpacity(layerOpacity[0]);
            if(withGeometry) {
                setMapBBOX(planForLandregisterMainPage,bbox);
            }
        }
        extract.setRealEstate(gs);
        
    }

    private Envelope getMapBBOX(Geometry parcelGeom) {
        Envelope bbox = parcelGeom.getEnvelopeInternal();
        double width=bbox.getWidth();
        double height=bbox.getHeight();
        double factor=Math.max(width/MAP_WIDTH_MM,height/MAP_HEIGHT_MM);
        bbox.expandBy((MAP_WIDTH_MM*factor-width)/2.0, (MAP_HEIGHT_MM*factor-height)/2.0);
        bbox.expandBy(5.0*factor, 5.0*factor);
        return bbox;
    }

    private byte[] getWmsImage(String fixedWmsUrl) 
        throws IOException, URISyntaxException 
    {
        byte ret[]=null;
        java.net.URL url=null;
        url=new java.net.URI(fixedWmsUrl).toURL();
        logger.trace("fetching <{}> ...",url);
        java.net.URLConnection conn=null;
        try {
            //
            // java  -Dhttp.proxyHost=myproxyserver.com  -Dhttp.proxyPort=80 MyJavaApp
            //
            // System.setProperty("http.proxyHost", "myProxyServer.com");
            // System.setProperty("http.proxyPort", "80");
            //
            // System.setProperty("java.net.useSystemProxies", "true");
            //
            // since 1.5 
            // Proxy instance, proxy ip = 123.0.0.1 with port 8080
            // Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress("123.0.0.1", 8080));
            // URL url = new URL("http://www.yahoo.com");
            // HttpURLConnection uc = (HttpURLConnection)url.openConnection(proxy);
            // uc.connect();
            // 
            conn = url.openConnection();
        } catch (IOException e) {
            throw e;
        }
        java.io.BufferedInputStream in=null;
        java.io.ByteArrayOutputStream fos=null;
        try{
            try {
                in=new java.io.BufferedInputStream(conn.getInputStream());
            } catch (IOException e) {
                throw e;
            }
            fos = new java.io.ByteArrayOutputStream();
            try {
                byte[] buf = new byte[1024];
                int i = 0;
                while ((i = in.read(buf)) != -1) {
                    fos.write(buf, 0, i);
                }
            } catch (IOException e) {
                throw e;
            }
            fos.flush();
            ret=fos.toByteArray();
        }finally{
            if(in!=null){
                try {
                    in.close();
                } catch (IOException e) {
                    logger.error("failed to close wms input stream",e);
                }
                in=null;
            }
            if(fos!=null){
                try {
                    fos.close();
                } catch (IOException e) {
                    logger.error("failed to close wms output stream",e);
                }
                fos=null;
            }
        }
        return ret;
    }

    private String getWmsUrl(Envelope bbox, String url,int dpi) {
        final UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(url);
        UriComponents uri=builder.build();
        String paramSrs=getWmsParam(uri.getQueryParams(), WMS_PARAM_SRS);
        if(uri.getQueryParams().containsKey(paramSrs)) {
            builder.replaceQueryParam(paramSrs,"EPSG:2056");
        }
        String paramBbox=getWmsParam(uri.getQueryParams(), WMS_PARAM_BBOX);
        builder.replaceQueryParam(paramBbox, bbox.getMinX()+","+bbox.getMinY()+","+bbox.getMaxX()+","+bbox.getMaxY());
        int mapWidthPixel = (int) (dpi*MAP_WIDTH_MM/25.4);
        int mapHeightPixel = (int) (dpi*MAP_HEIGHT_MM/25.4);
        
        String paramDpi=getWmsParam(uri.getQueryParams(), WMS_PARAM_DPI);
        builder.replaceQueryParam(paramDpi, dpi);
        String paramHeight=getWmsParam(uri.getQueryParams(), WMS_PARAM_HEIGHT);
        builder.replaceQueryParam(paramHeight, mapHeightPixel);
        String paramWidth=getWmsParam(uri.getQueryParams(), WMS_PARAM_WIDTH);
        builder.replaceQueryParam(paramWidth, mapWidthPixel);
        String fixedWmsUrl = builder.build().toUriString();
        return fixedWmsUrl;
    }
    private Integer getLayerIndex(String url, double[] layerOpacity) {
        UriComponents builder = UriComponentsBuilder.fromUriString(url).build();
        String paramLayers=getWmsParam(builder.getQueryParams(),WMS_PARAM_LAYERS);
        List<String> layers=new ArrayList<String>(builder.getQueryParams().get(paramLayers));
        layers.sort(null);
        java.util.List<java.util.Map<String,Object>> wmsv=jdbcTemplate.queryForList(
                "SELECT verweiswms, layerindex, layerdeckkraft FROM "+getSchema()+"."+OERBKRMVS_V2_0KONFIGURATION_MAPLAYERING);
        for(java.util.Map<String,Object> wmsData:wmsv) {
            UriComponents wmsUrlBuilder = UriComponentsBuilder.fromUriString((String)wmsData.get("verweiswms")).build();
            paramLayers=getWmsParam(wmsUrlBuilder.getQueryParams(),WMS_PARAM_LAYERS);
            List<String> wmsLayers=new ArrayList<String>(wmsUrlBuilder.getQueryParams().get(paramLayers));
            wmsLayers.sort(null);
            if(wmsLayers.equals(layers)) {
                layerOpacity[0]=((BigDecimal) wmsData.get("layerdeckkraft")).doubleValue();
                return (Integer) wmsData.get("layerindex");
            }
        }
        return null;
    }
    private String getWmsParam(MultiValueMap<String, String> queryParams, String param) {
        for(String queryParam:queryParams.keySet()) {
            if(queryParam.equalsIgnoreCase(param)) {
                return queryParam;
            }
        }
        return param;
    }
    private List<TopicCode> sortTopics(List<TopicCode> topics, Map<TopicCode, Integer> topicOrdering) {
        List<TopicCode> ret=new ArrayList<TopicCode>(topics);
        ret.sort(new Comparator<TopicCode>() {

            @Override
            public int compare(TopicCode o1, TopicCode o2) {
                int idx1=topicOrdering.get(o1);
                int idx2=topicOrdering.get(o2);
                return Integer.compare(idx1, idx2);
            }
            
        });
        return ret;
    }
    private Map<TopicCode, Integer> getTopicOrdering() {
        java.util.Map<TopicCode,Integer> ret=new java.util.HashMap<TopicCode,Integer>();
        jdbcTemplate.query(
                "SELECT acode,subcode,auszugindex FROM "+getSchema()+"."+OERBKRMVS_V2_0THEMA_THEMA,new RowCallbackHandler() {
                    @Override
                    public void processRow(ResultSet rs) throws SQLException {
                        String code=rs.getString("acode");
                        String subcode=rs.getString("subcode");
                        int index=rs.getInt("auszugindex");
                        TopicCode topic=new TopicCode(code,subcode);
                        ret.put(topic,index);
                    }
                });
        return ret;
    }


    private List<TopicCode> parseParameterTopics(String requestedTopicsAsText) {
        if(requestedTopicsAsText==null || requestedTopicsAsText.length()==0) {
            requestedTopicsAsText=PARAM_CONST_ALL;
        }
        java.util.Set<TopicCode> all=new java.util.HashSet<TopicCode>();
        java.util.Set<TopicCode> allMain=new java.util.HashSet<TopicCode>();
        java.util.Map<String,TopicCode> allSub=new java.util.HashMap<String,TopicCode>();
        jdbcTemplate.query(
                "SELECT acode,subcode FROM "+getSchema()+"."+OERBKRMVS_V2_0THEMA_THEMA,new RowCallbackHandler() {
                    @Override
                    public void processRow(ResultSet rs) throws SQLException {
                        String code=rs.getString("acode");
                        String subcode=rs.getString("subcode");
                        TopicCode topic=new TopicCode(code,subcode);
                        all.add(topic);
                        if(!topic.isSubTopic()) {
                            allMain.add(topic);
                        }else {
                            allSub.put(topic.getSubCode(), topic);
                        }
                    }
                });
        java.util.Set<TopicCode> ret=new java.util.HashSet<TopicCode>();
        String topicsx[]=requestedTopicsAsText.split(";");
        for(String topicCode:topicsx) {
            if(topicCode.equals(PARAM_CONST_ALL_FEDERAL)) {
                for(TopicCode code:allMain) {
                    if(isFederalTopicCode(code.getMainCode())) {
                        ret.add(code);
                    }
                }
            }else if(topicCode.equals(PARAM_CONST_ALL)){
                ret.addAll(all);
            }else {
                if(allSub.containsKey(topicCode)) {
                    ret.add(allSub.get(topicCode));
                }else {
                    ret.add(new TopicCode(topicCode,null));
                }
            }
            
        }
        return new ArrayList<TopicCode>(ret);
    }
    private boolean isFederalTopicCode(String code) {
        return code.startsWith("ch.") && code.indexOf('.', 3)==-1;        
    }
    private MultilingualTextType getTopicText(TopicCode code) {
        String title_de=null;
        try {
            if(code.isSubTopic()) {
                title_de=jdbcTemplate.queryForObject(
                        "SELECT titel_de FROM "+getSchema()+"."+OERBKRMVS_V2_0THEMA_THEMA+" WHERE acode=? AND subcode=?",String.class,code.getMainCode(),code.getSubCode());
            }else {
                title_de=jdbcTemplate.queryForObject(
                        "SELECT titel_de FROM "+getSchema()+"."+OERBKRMVS_V2_0THEMA_THEMA+" WHERE (acode=? AND subcode IS NULL)",String.class,code.getMainCode());
            }
        }catch(EmptyResultDataAccessException ex) {
            logger.error("unknown topic code <{}>",code);
            title_de="Thematitel";
        }
        LocalisedTextType text=new LocalisedTextType();
        text.setLanguage(LanguageCodeType.DE);
        text.setText(title_de);
        MultilingualTextType ret=new MultilingualTextType();
        ret.getLocalisedText().add(text);
        return ret;
    }
    private String  verifyEgrid(String egrid,String identdn,String number) {
        try {
            String ret=jdbcTemplate.queryForObject(
                    "SELECT egris_egrid AS type FROM "+getSchema()+"."+TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_GRUNDSTUECK+" WHERE egris_egrid=? OR (nummer=? AND nbident=?)", String.class,egrid,number,identdn);
            return ret;
        }catch(EmptyResultDataAccessException ex) {
        }
        return null;
    }
    

    private List<TopicCode> getTopicsOfMunicipality(int bfsNr) {
        List<TopicCode> ret=new ArrayList<TopicCode>();
        jdbcTemplate.query("SELECT c.thema,c.subthema from "+getSchema()+"."+OEREBKRM_V2_0_THEMAREF+" as c" 
                + " JOIN "+getSchema()+"."+OERBKRMVS_V2_0KONFIGURATION_GEMEINDEMITOEREBK+" as m On c.oerbkrmvs_v_gmndmtrebk_themen=m.t_id"
                        + " WHERE m.gemeinde=?",new RowCallbackHandler() {
                            @Override
                            public void processRow(ResultSet rs) throws SQLException {
                                String code=rs.getString("thema");
                                String subcode=rs.getString("subthema");
                                ret.add(new TopicCode(code,subcode));
                            }
                        },bfsNr);
        return ret;
    }
    private java.sql.Date getBasedatadateOfMunicipality(int bfsNr) {
        java.sql.Date ret=null;
        try {
            ret=jdbcTemplate.queryForObject("SELECT grundlagedatenstand from "+getSchema()+"."+OERBKRMVS_V2_0KONFIGURATION_GEMEINDEMITOEREBK+" WHERE gemeinde=?",java.sql.Date.class,bfsNr);
        }catch(EmptyResultDataAccessException ex) {
            // a non-unlocked municipality has no entry
            return null;
        }
        if(ret==null) {
            ret=new java.sql.Date(System.currentTimeMillis());
        }
        return ret;
    }
    private String getSubunitDesignationOfMunicipality(int bfsNr) {
        String ret=null;
        try {
            ret=jdbcTemplate.queryForObject("SELECT bezeichnunguntereinheitgrundbuch from "+getSchema()+"."+OERBKRMVS_V2_0KONFIGURATION_GEMEINDEMITOEREBK+" WHERE gemeinde=?",String.class,bfsNr);
        }catch(EmptyResultDataAccessException ex) {
            // a non-unlocked municipality has no entry
            return null;
        }
        return ret;
    }
    private List<TopicCode> getAllTopicsOfThisCadastre() {
        List<TopicCode> ret=new ArrayList<TopicCode>();
        jdbcTemplate.query("SELECT DISTINCT acode,subcode from "+getSchema()+"."+OERBKRMVS_V2_0THEMA_THEMA
                        ,new RowCallbackHandler() {
                            @Override
                            public void processRow(ResultSet rs) throws SQLException {
                                String code=rs.getString("acode");
                                String subcode=rs.getString("subcode");
                                ret.add(new TopicCode(code,subcode));
                            }
                        });
        return ret;
    }
    
    @Scheduled(cron="0 * * * * *")
    private void cleanUp() {    
        java.io.File[] tmpDirs = new java.io.File(oerebTmpdir).listFiles();
        if(tmpDirs!=null) {
            for (java.io.File tmpDir : tmpDirs) {
                if (tmpDir.getName().startsWith(TMP_FOLDER_PREFIX)) {
                    try {
                        FileTime creationTime = (FileTime) Files.getAttribute(Paths.get(tmpDir.getAbsolutePath()), "creationTime");                    
                        Instant now = Instant.now();
                        
                        long fileAge = now.getEpochSecond() - creationTime.toInstant().getEpochSecond();
                        if (fileAge > 60*60) {
                            logger.info("deleting {}", tmpDir.getAbsolutePath());
                            FileSystemUtils.deleteRecursively(tmpDir);
                        }
                    } catch (IOException e) {
                        throw new IllegalStateException(e);
                    }
                }
            }
        }
    }
}