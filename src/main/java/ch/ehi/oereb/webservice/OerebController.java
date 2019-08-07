package ch.ehi.oereb.webservice;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.xml.bind.JAXBElement;
import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import javax.xml.namespace.QName;

import org.apache.commons.io.FilenameUtils;
import org.apache.tomcat.util.net.SSLHostConfigCertificate.Type;
import org.hibernate.validator.internal.util.logging.LoggerFactory;
import org.postgresql.util.Base64;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.InputStreamResource;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.SqlParameterValue;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.util.UriComponentsBuilder;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.io.ByteOrderValues;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;

import ch.ehi.oereb.schemas.gml._3_2.MultiSurface;
import ch.ehi.oereb.schemas.gml._3_2.MultiSurfacePropertyTypeType;
import ch.ehi.oereb.schemas.gml._3_2.MultiSurfaceTypeType;
import ch.ehi.oereb.schemas.gml._3_2.Point;
import ch.ehi.oereb.schemas.gml._3_2.PointPropertyTypeType;
import ch.ehi.oereb.schemas.gml._3_2.PointTypeType;
import ch.ehi.oereb.schemas.gml._3_2.Pos;
import ch.ehi.oereb.schemas.gml._3_2.SurfacePropertyTypeType;
import ch.ehi.oereb.schemas.oereb._1_0.extract.GetCapabilitiesResponse;
import ch.ehi.oereb.schemas.oereb._1_0.extract.GetCapabilitiesResponseType;
import ch.ehi.oereb.schemas.oereb._1_0.extract.GetEGRIDResponse;
import ch.ehi.oereb.schemas.oereb._1_0.extract.GetEGRIDResponseType;
import ch.ehi.oereb.schemas.oereb._1_0.extract.GetExtractByIdResponse;
import ch.ehi.oereb.schemas.oereb._1_0.extract.GetExtractByIdResponseType;
import ch.ehi.oereb.schemas.oereb._1_0.extractdata.CantonCodeType;
import ch.ehi.oereb.schemas.oereb._1_0.extractdata.DocumentBaseType;
import ch.ehi.oereb.schemas.oereb._1_0.extractdata.DocumentType;
import ch.ehi.oereb.schemas.oereb._1_0.extractdata.ExclusionOfLiabilityType;
import ch.ehi.oereb.schemas.oereb._1_0.extractdata.Extract;
import ch.ehi.oereb.schemas.oereb._1_0.extractdata.ExtractType;
import ch.ehi.oereb.schemas.oereb._1_0.extractdata.GeometryType;
import ch.ehi.oereb.schemas.oereb._1_0.extractdata.GlossaryType;
import ch.ehi.oereb.schemas.oereb._1_0.extractdata.LanguageCodeType;
import ch.ehi.oereb.schemas.oereb._1_0.extractdata.LawstatusCodeType;
import ch.ehi.oereb.schemas.oereb._1_0.extractdata.LawstatusType;
import ch.ehi.oereb.schemas.oereb._1_0.extractdata.LegendEntryType;
import ch.ehi.oereb.schemas.oereb._1_0.extractdata.LocalisedMTextType;
import ch.ehi.oereb.schemas.oereb._1_0.extractdata.LocalisedTextType;
import ch.ehi.oereb.schemas.oereb._1_0.extractdata.LocalisedUriType;
import ch.ehi.oereb.schemas.oereb._1_0.extractdata.MapType;
import ch.ehi.oereb.schemas.oereb._1_0.extractdata.MultilingualMTextType;
import ch.ehi.oereb.schemas.oereb._1_0.extractdata.MultilingualTextType;
import ch.ehi.oereb.schemas.oereb._1_0.extractdata.MultilingualUriType;
import ch.ehi.oereb.schemas.oereb._1_0.extractdata.OfficeType;
import ch.ehi.oereb.schemas.oereb._1_0.extractdata.RealEstateDPRType;
import ch.ehi.oereb.schemas.oereb._1_0.extractdata.RealEstateTypeType;
import ch.ehi.oereb.schemas.oereb._1_0.extractdata.RestrictionOnLandownershipType;
import ch.ehi.oereb.schemas.oereb._1_0.extractdata.ThemeType;
import ch.ehi.oereb.schemas.oereb._1_0.extractdata.WebReferenceType;
import ch.ehi.oereb.schemas.oereb._1_0.versioning.GetVersionsResponse;
import ch.ehi.oereb.schemas.oereb._1_0.versioning.GetVersionsResponseType;
import ch.ehi.oereb.schemas.oereb._1_0.versioning.VersionType;
import ch.so.agi.oereb.pdf4oereb.ConverterException;
import ch.so.agi.oereb.pdf4oereb.Locale;
// http://localhost:8080/extract/reduced/xml/geometry/CH693289470668


@Controller
public class OerebController {
    
    private static final String TABLE_OEREBKRM_V1_1_LOCALISEDURI = "oerebkrm_v1_1_localiseduri";
    private static final String TABLE_OEREBKRM_V1_1_MULTILINGUALURI = "oerebkrm_v1_1_multilingualuri";
    private static final String TABLE_OEREBKRM_V1_1CODELISTENTEXT_RECHTSSTATUSTXT = "oerebkrm_v1_1codelistentext_rechtsstatustxt";
    private static final String TABLE_OERBKRMVS_V1_1VORSCHRIFTEN_DOKUMENT = "oerbkrmvs_v1_1vorschriften_dokument";
    private static final String TABLE_OERBKRMFR_V1_1TRANSFERSTRUKTUR_HINWEISVORSCHRIFT = "oerbkrmfr_v1_1transferstruktur_hinweisvorschrift";
    private static final String TABLE_OERBKRMVS_V1_1VORSCHRIFTEN_HINWEISWEITEREDOKUMENTE = "oerbkrmvs_v1_1vorschriften_hinweisweiteredokumente";
    private static final String TABLE_OERBKRMFR_V1_1TRANSFERSTRUKTUR_LEGENDEEINTRAG = "oerbkrmfr_v1_1transferstruktur_legendeeintrag";
    private static final String TABLE_OERB_XTNX_V1_0ANNEX_THEMATXT = "oerb_xtnx_v1_0annex_thematxt";
    private static final String TABLE_OERB_XTNX_V1_0ANNEX_MUNICIPALITYWITHPLRC = "oerb_xtnx_v1_0annex_municipalitywithplrc";
    private static final String TABLE_DM01VCH24LV95DGEMEINDEGRENZEN_GEMEINDE = "dm01vch24lv95dgemeindegrenzen_gemeinde";
    private static final String TABLE_SO_G_V_0180822GRUNDBUCHKREISE_GRUNDBUCHKREIS = "so_g_v_0180822grundbuchkreise_grundbuchkreis";
    private static final String TABLE_OERB_XTNX_V1_0ANNEX_BASEDATA = "oerb_xtnx_v1_0annex_basedata";
    private static final String TABLE_OERB_XTNX_V1_0ANNEX_GENERALINFORMATION = "oerb_xtnx_v1_0annex_generalinformation";
    private static final String TABLE_OERB_XTNX_V1_0ANNEX_EXCLUSIONOFLIABILITY = "oerb_xtnx_v1_0annex_exclusionofliability";
    private static final String TABLE_OERB_XTNX_V1_0ANNEX_GLOSSARY = "oerb_xtnx_v1_0annex_glossary";
    private static final String TABLE_OERB_XTNX_V1_0ANNEX_OFFICE = "oerb_xtnx_v1_0annex_office";
    private static final String TABLE_OERB_XTNX_V1_0ANNEX_LOGO = "oerb_xtnx_v1_0annex_logo";
    private static final String TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_LIEGENSCHAFT = "dm01vch24lv95dliegenschaften_liegenschaft";
    private static final String TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_GRUNDSTUECK = "dm01vch24lv95dliegenschaften_grundstueck";
    private static final String TABLE_OEREBKRM_V1_1CODELISTENTEXT_THEMATXT = "oerebkrm_v1_1codelistentext_thematxt";
    private static final String TABLE_OEREB_EXTRACTANNEX_V1_0_CODE = "oereb_extractannex_v1_0_code_";

    protected static final String extractNS = "http://schemas.geo.admin.ch/V_D/OeREB/1.0/Extract";
    private static final LanguageCodeType DE = LanguageCodeType.DE;
    
    private Logger logger=org.slf4j.LoggerFactory.getLogger(this.getClass());
    private Jts2GML32 jts2gml = new Jts2GML32();
    
    @Autowired
    JdbcTemplate jdbcTemplate;
    
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
    @Value("${oereb.tmpdir}")
    private String oerebTmpdir;
    
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
    
    /* 
     * https://example.com/oereb/getegrid/xml/?XY=608000,228000
     * https://example.com/oereb/getegrid/json/BE0200000332/100
     * https://example.com/oereb/getegrid/json/3084/Lindenweg/50
     * https://example.com/oereb/getegrid/xml/?GNSS=46.94890,7.44665
     * ${baseurl}/getegrid/${FORMAT}/?XY=${XY}/
     * ${baseurl}/getegrid/${FORMAT}/${IDENTDN}/${NUMBER}
     * ${baseurl}/getegrid/${FORMAT}/${POSTALCODE}/${LOCALISATION}/${NUMBER}
     * ${baseurl}/getegrid/${FORMAT}/?GNSS=${GNSS}
     */
    @GetMapping("/getegrid/{format}/{identdn}/{number}")
    public ResponseEntity<GetEGRIDResponse>  getEgridByNumber(@PathVariable String format, @PathVariable String identdn,@PathVariable String number) {
        if(!format.equals("xml")) {
            throw new IllegalArgumentException("unsupported format <"+format+">");
        }
        GetEGRIDResponseType ret= new GetEGRIDResponseType();
        List<JAXBElement<String>[]> gsList=jdbcTemplate.query(
                "SELECT egris_egrid,nummer,nbident FROM "+getSchema()+"."+TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_GRUNDSTUECK+" WHERE nummer=? AND nbident=?", new RowMapper<JAXBElement<String>[]>() {
                    @Override
                    public JAXBElement<String>[] mapRow(ResultSet rs, int rowNum) throws SQLException {
                        JAXBElement<String> ret[]=new JAXBElement[3];
                        ret[0]=new JAXBElement<String>(new QName(extractNS,"egrid"),String.class,rs.getString(1));
                        ret[1]=new JAXBElement<String>(new QName(extractNS,"number"),String.class,rs.getString(2));
                        ret[2]=new JAXBElement<String>(new QName(extractNS,"identDN"),String.class,rs.getString(3));
                        return ret;
                    }
                    
                },number,identdn);
        for(JAXBElement<String>[] gs:gsList) {
            ret.getEgridAndNumberAndIdentDN().add(gs[0]);
            ret.getEgridAndNumberAndIdentDN().add(gs[1]);
            ret.getEgridAndNumberAndIdentDN().add(gs[2]);
        }
         return new ResponseEntity<GetEGRIDResponse>(new GetEGRIDResponse(ret),gsList.size()>0?HttpStatus.OK:HttpStatus.NO_CONTENT);
    }
    @GetMapping("/getegrid/{format}")
    public ResponseEntity<GetEGRIDResponse>  getEgridByXY(@PathVariable String format,@RequestParam(value="XY", required=false) String xy,@RequestParam(value="GNSS", required=false) String gnss) {
        if(!format.equals("xml")) {
            throw new IllegalArgumentException("unsupported format <"+format+">");
        }
        if(xy==null && gnss==null) {
            throw new IllegalArgumentException("parameter XY or GNSS required");
        }else if(xy!=null && gnss!=null) {
            throw new IllegalArgumentException("only one of parameters XY or GNSS is allowed");
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
        List<JAXBElement<String>[]> gsList=jdbcTemplate.query(
                "SELECT egris_egrid,nummer,nbident FROM "+getSchema()+"."+TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_GRUNDSTUECK+" g LEFT JOIN "+getSchema()+"."+TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_LIEGENSCHAFT+" l ON l.liegenschaft_von=g.t_id WHERE ST_DWithin(ST_Transform(?,2056),l.geometrie,1.0)", new RowMapper<JAXBElement<String>[]>() {
                    @Override
                    public JAXBElement<String>[] mapRow(ResultSet rs, int rowNum) throws SQLException {
                        JAXBElement<String> ret[]=new JAXBElement[3];
                        ret[0]=new JAXBElement<String>(new QName(extractNS,"egrid"),String.class,rs.getString(1));
                        ret[1]=new JAXBElement<String>(new QName(extractNS,"number"),String.class,rs.getString(2));
                        ret[2]=new JAXBElement<String>(new QName(extractNS,"identDN"),String.class,rs.getString(3));
                        return ret;
                    }
                    
                },geom);
        for(JAXBElement<String>[] gs:gsList) {
            ret.getEgridAndNumberAndIdentDN().add(gs[0]);
            ret.getEgridAndNumberAndIdentDN().add(gs[1]);
            ret.getEgridAndNumberAndIdentDN().add(gs[2]);
        }
         return new ResponseEntity<GetEGRIDResponse>(new GetEGRIDResponse(ret),gsList.size()>0?HttpStatus.OK:HttpStatus.NO_CONTENT);
    }
    
    /*
     * 
     *   https://example.com/oereb/extract/reduced/xml/CH887722167773
     *   https://example.com/oereb/extract/reduced/xml/geometry/CH887722167773
     *   https://example.com/oereb/extract/full/pdf/BE0200000332/100
     *   ${baseurl}/extract/${FLAVOUR}/${FORMAT}[/${GEOMETRY}]/${EGRID}[?LANG=${LANG}&TOPICS=${TOPICS}&WITHIMAGES]
     *   ${baseurl}/extract/${FLAVOUR}/${FORMAT}[/${GEOMETRY}]/${IDENTDN}/${NUMBER}[?LANG=${LANG}&TOPICS=${TOPICS}&WITHIMAGES]
     */
                
    @GetMapping(value="/extract/reduced/{format}/{geometry}/{egrid}",consumes=MediaType.ALL_VALUE,produces = {MediaType.APPLICATION_PDF_VALUE,MediaType.APPLICATION_XML_VALUE})
    public ResponseEntity<?>  getExtractWithGeometryByEgrid(@PathVariable String format,@PathVariable String geometry,@PathVariable String egrid,@RequestParam(value="LANG", required=false) String lang,@RequestParam(value="TOPICS", required=false) String topics,@RequestParam(value="WITHIMAGES", required=false) String withImages) {
        if(!format.equals("xml") && !format.equals("pdf")) {
            throw new IllegalArgumentException("unsupported format <"+format+">");
        }
        Grundstueck parcel=getParcelByEgrid(egrid);
        if(parcel==null) {
            return new ResponseEntity<Object>(HttpStatus.NO_CONTENT);
        }
        Extract extract=createExtract(egrid,parcel,true,lang,topics,withImages==null?false:true);
        
        GetExtractByIdResponseType response=new GetExtractByIdResponseType();
        response.setExtract(extract);
        GetExtractByIdResponse responseEle=new GetExtractByIdResponse(response);
        
        if(format.equals("pdf")) {
            String tmpdir = oerebTmpdir;
            if(tmpdir==null) {
                tmpdir=System.getProperty("java.io.tmpdir");
            }
            java.io.File tmpFolder=new java.io.File(tmpdir,"oerebws"+Thread.currentThread().getId());
            if(!tmpFolder.exists()) {
                tmpFolder.mkdirs();
            }
            logger.info("tmpFolder {}",tmpFolder.getAbsolutePath());
            java.io.File tmpExtractFile=new java.io.File(tmpFolder,egrid+".xml");
            marshaller.marshal(responseEle,new javax.xml.transform.stream.StreamResult(tmpExtractFile));
            try {
                java.io.File pdfFile=extractXml2pdf.runXml2Pdf(tmpExtractFile.getAbsolutePath(), tmpFolder.getAbsolutePath(), Locale.DE);
                String pdfFilename = pdfFile.getName();
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
        return new ResponseEntity<GetExtractByIdResponse>(responseEle,HttpStatus.OK);
    }    

    @GetMapping("/extract/reduced/{format}/{egrid}")
    public ResponseEntity<?>  getExtractWithoutGeometryByEgrid(@PathVariable String format,@PathVariable String geometry,@PathVariable String egrid,@RequestParam(value="LANG", required=false) String lang,@RequestParam(value="TOPICS", required=false) String topics,@RequestParam(value="WITHIMAGES", required=false) String withImages) {
        if(!format.equals("xml")) {
            throw new IllegalArgumentException("unsupported format <"+format+">");
        }
        return null;
    }    
    @GetMapping("/extract/reduced/{format}/{geometry}/{identdn}/{number}")
    public ResponseEntity<?>  getExtractWithGeometryByNumber(@PathVariable String format,@PathVariable String geometry,@PathVariable String identdn,@PathVariable String number,@RequestParam(value="LANG", required=false) String lang,@RequestParam(value="TOPICS", required=false) String topics,@RequestParam(value="WITHIMAGES", required=false) String withImages) {
        if(!format.equals("xml")) {
            throw new IllegalArgumentException("unsupported format <"+format+">");
        }
        return null;
    }    
    @GetMapping("/capabilities/{format}")
    public @ResponseBody  GetCapabilitiesResponse getCapabilities(@PathVariable String format) {
        if(!format.equals("xml")) {
            throw new IllegalArgumentException("unsupported format <"+format+">");
        }
        GetCapabilitiesResponseType ret=new GetCapabilitiesResponseType();
        
        // Liste der vorhandenen OeREB-Katasterthemen (inkl. Kantons- und Gemeindethemen);
        setThemes(ret.getTopic(),getAllTopicsOfThisCadastre());
        
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
        ret.getFlavour().add("reduced");
        // Liste der unterstuetzten Sprachen (2 stellige ISO Codes);
        ret.getLanguage().add("de");
        // Liste der unterstuetzten CRS.
        ret.getCrs().add("2056");
        return new GetCapabilitiesResponse(ret);
    }
    @GetMapping("/versions/{format}")
    public @ResponseBody  GetVersionsResponse getVersions(@PathVariable String format) {
        if(!format.equals("xml")) {
            throw new IllegalArgumentException("unsupported format <"+format+">");
        }
        GetVersionsResponseType ret=new GetVersionsResponseType();
        VersionType ver=new VersionType();
        ver.setVersion("extract-1.0");
        ret.getSupportedVersion().add(ver);
        return new GetVersionsResponse(ret);
    }
    
    private Extract createExtract(String egrid, Grundstueck parcel, boolean withGeometry, String lang, String requestedTopicsAsText, boolean withImages) {
        ExtractType extract=new ExtractType();
        extract.setIsReduced(true);
        XMLGregorianCalendar today=null;
        try {
            GregorianCalendar gdate=new GregorianCalendar();
            gdate.setTime(new java.util.Date());
            today = DatatypeFactory.newInstance().newXMLGregorianCalendar(gdate);
        } catch (DatatypeConfigurationException e) {
            throw new IllegalStateException(e);
        }
        extract.setCreationDate(today);
        extract.setExtractIdentifier(UUID.randomUUID().toString());
        List<String> requestedTopics=parseTopics(requestedTopicsAsText);
        // Grundstueck
        final Geometry parcelGeom = parcel.getGeometrie();
        Envelope bbox = getMapBBOX(parcelGeom);
        setParcel(extract,egrid,parcel,bbox,withGeometry);
        int bfsNr=extract.getRealEstate().getFosNr();
        // freigeschaltete Themen in der betroffenen Gemeinde
        List<String> availableTopics=getTopicsOfMunicipality(bfsNr);
        List<String> queryTopics=new ArrayList<String>();
        queryTopics.addAll(availableTopics);
        queryTopics.retainAll(requestedTopics);
        List<String> concernedTopics=new ArrayList<String>();

        addRestrictions(extract,parcelGeom,bbox,withGeometry,withImages,queryTopics,concernedTopics);
        // Themen
        List<String> themeWithoutData=new ArrayList<String>();
        themeWithoutData.addAll(requestedTopics);
        themeWithoutData.removeAll(availableTopics);
        List<String> notConcernedTopics=new ArrayList<String>();
        notConcernedTopics.addAll(queryTopics);
        notConcernedTopics.removeAll(concernedTopics);
        setThemes(extract.getConcernedTheme(), concernedTopics);
        setThemes(extract.getNotConcernedTheme(), notConcernedTopics);
        setThemes(extract.getThemeWithoutData(), themeWithoutData);
        // Logos
        extract.setLogoPLRCadastre(getImage("ch.plr"));
        extract.setFederalLogo(getImage("ch"));
        extract.setCantonalLogo(getImage("ch."+extract.getRealEstate().getCanton().name().toLowerCase()));
        extract.setMunicipalityLogo(getImage("ch."+extract.getRealEstate().getFosNr()));
        // Text
        setBaseData(extract);
        setGeneralInformation(extract);
        setExclusionOfLiability(extract);
        setGlossary(extract);
        // Oereb-Amt
        OfficeType plrCadastreAuthority = new OfficeType();
        WebReferenceType webRef=new WebReferenceType();
        webRef.setValue(plrCadastreAuthorityUrl);
        plrCadastreAuthority.setOfficeAtWeb(webRef);

        setOffice(plrCadastreAuthority);
        extract.setPLRCadastreAuthority(plrCadastreAuthority);
        
        return new Extract(extract);
    }

    private byte[] getImage(String code) {
        java.util.List<byte[]> baseData=jdbcTemplate.queryForList(
                "SELECT logo FROM "+getSchema()+"."+TABLE_OERB_XTNX_V1_0ANNEX_LOGO+" WHERE acode=?",byte[].class,code);
        if(baseData!=null && baseData.size()==1) {
            return baseData.get(0);
        }
        return minimalImage;
    }

    private void setOffice(OfficeType office) {
        java.util.Map<String,Object> baseData=jdbcTemplate.queryForMap(
                "SELECT aname_de,auid,line1,line2,street,anumber,postalcode,city FROM "+getSchema()+"."+TABLE_OERB_XTNX_V1_0ANNEX_OFFICE+" WHERE officeatweb=?",office.getOfficeAtWeb().getValue());
        if(baseData!=null) {
            office.setName(createMultilingualTextType(baseData, "aname"));
            office.setUID((String) baseData.get("auid"));
            office.setLine1((String) baseData.get("line2"));
            office.setLine2((String) baseData.get("line1"));
            office.setStreet((String) baseData.get("street"));
            office.setNumber((String) baseData.get("anumber"));
            office.setPostalCode((String) baseData.get("postalcode"));
            office.setCity((String) baseData.get("city"));
        }
    }

    private void setGlossary(ExtractType extract) {
        java.util.List<java.util.Map<String,Object>> baseDataList=jdbcTemplate.queryForList(
                "SELECT title_de,title_fr,title_it,title_rm,title_en,content_de,content_fr,content_it,content_rm,content_en FROM "+getSchema()+"."+TABLE_OERB_XTNX_V1_0ANNEX_GLOSSARY);
        for(java.util.Map<String,Object> baseData:baseDataList) {
            MultilingualMTextType content = createMultilingualMTextType(baseData,"content");
            MultilingualTextType title = createMultilingualTextType(baseData,"title");
            GlossaryType glossary=new GlossaryType();
            glossary.setContent(content);
            glossary.setTitle(title);
            extract.getGlossary().add(glossary);
        }
    }

    private void setExclusionOfLiability(ExtractType extract) {
        java.util.List<java.util.Map<String,Object>> baseDataList=jdbcTemplate.queryForList(
                "SELECT title_de,title_fr,title_it,title_rm,title_en,content_de,content_fr,content_it,content_rm,content_en FROM "+getSchema()+"."+TABLE_OERB_XTNX_V1_0ANNEX_EXCLUSIONOFLIABILITY);
        for(java.util.Map<String,Object> baseData:baseDataList) {
            MultilingualMTextType content = createMultilingualMTextType(baseData,"content");
            MultilingualTextType title = createMultilingualTextType(baseData,"title");
            ExclusionOfLiabilityType exclOfLiab=new ExclusionOfLiabilityType();
            exclOfLiab.setContent(content);
            exclOfLiab.setTitle(title);
            extract.getExclusionOfLiability().add(exclOfLiab);
        }
    }

    private void setGeneralInformation(ExtractType extract) {
        java.util.Map<String,Object> baseData=jdbcTemplate.queryForMap(
                "SELECT content_de,content_fr,content_it,content_rm,content_en FROM "+getSchema()+"."+TABLE_OERB_XTNX_V1_0ANNEX_GENERALINFORMATION);
        extract.setGeneralInformation(createMultilingualMTextType(baseData,"content"));
    }

    private void setBaseData(ExtractType extract) {
        java.util.Map<String,Object> baseData=jdbcTemplate.queryForMap(
                "SELECT content_de,content_fr,content_it,content_rm,content_en FROM "+getSchema()+"."+TABLE_OERB_XTNX_V1_0ANNEX_BASEDATA);
        extract.setBaseData(createMultilingualMTextType(baseData,"content"));
    }

    private MultilingualMTextType createMultilingualMTextType(Map<String, Object> baseData,String prefix) {
        MultilingualMTextType ret=new MultilingualMTextType();
        for(LanguageCodeType lang:LanguageCodeType.values()) {
            String txt=(String)baseData.get(prefix+"_"+lang.value());
            if(txt!=null) {
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
    private MultilingualMTextType createMultilingualMTextType(String txt) {
        MultilingualMTextType ret=new MultilingualMTextType();
        LocalisedMTextType lTxt = createLocalizedMText(txt);
        ret.getLocalisedText().add(lTxt);
        return ret;
    }
    private MultilingualTextType createMultilingualTextType(String txt) {
        MultilingualTextType ret=new MultilingualTextType();
        LocalisedTextType lTxt = createLocalizedText(txt);
        ret.getLocalisedText().add(lTxt);
        return ret;
    }
    private MultilingualUriType createMultilinualUriType(String txt) {
        MultilingualUriType ret=new MultilingualUriType();
        LocalisedUriType lTxt = createLocalizedUri(txt);
        ret.getLocalisedText().add(lTxt);
        return ret;
    }

    private LocalisedMTextType createLocalizedMText(String txt) {
        LocalisedMTextType lTxt= new LocalisedMTextType();
        lTxt.setLanguage(DE);
        lTxt.setText(txt);
        return lTxt;
    }
    private LocalisedTextType createLocalizedText(String txt) {
        LocalisedTextType lTxt= new LocalisedTextType();
        lTxt.setLanguage(DE);
        lTxt.setText(txt);
        return lTxt;
    }
    private LocalisedUriType createLocalizedUri(String txt) {
        LocalisedUriType lTxt= new LocalisedUriType();
        lTxt.setLanguage(DE);
        lTxt.setText(txt);
        return lTxt;
    }
    protected WebReferenceType createWebReferenceType(String url) {
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
                "SELECT ST_AsBinary(geometrie),nummer,nbident,art,gesamteflaechenmass,flaechenmass FROM "+getSchema()+"."+TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_GRUNDSTUECK+" g LEFT JOIN "+getSchema()+"."+TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_LIEGENSCHAFT+" l ON g.t_id=l.liegenschaft_von WHERE g.egris_egrid=?", new RowMapper<Grundstueck>() {
                    WKBReader decoder=new WKBReader(geomFactory);
                    
                    @Override
                    public Grundstueck mapRow(ResultSet rs, int rowNum) throws SQLException {
                        Geometry polygon=null;
                        try {
                            polygon=decoder.read(rs.getBytes(1));
                            if(polygon==null || polygon.isEmpty()) {
                                return null;
                            }
                        } catch (ParseException e) {
                            throw new IllegalStateException(e);
                        }
                        Grundstueck ret=new Grundstueck();
                        ret.setGeometrie(polygon);
                        ret.setEgrid(egrid);
                        ret.setNummer(rs.getString(2));
                        ret.setNbident(rs.getString(3));
                        ret.setArt(rs.getString(4));
                        int f=rs.getInt(5);
                        if(rs.wasNull()) {
                            f=rs.getInt(6);
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
        return gs;
    }
    private Geometry getParcelGeometryByEgrid(String egrid) {
        byte[] geom=jdbcTemplate.queryForObject(
                "SELECT ST_AsBinary(ST_Collect(geometrie)) FROM "+getSchema()+"."+TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_GRUNDSTUECK+" g LEFT JOIN "+getSchema()+"."+TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_LIEGENSCHAFT+" l ON g.egris_egrid=?", new RowMapper<byte[]>() {
                    @Override
                    public byte[] mapRow(ResultSet rs, int rowNum) throws SQLException {
                        return rs.getBytes(1);
                    }
                    
                },egrid);
        if(geom==null) {
            return null;
        }
        PrecisionModel precisionModel=new PrecisionModel(1000.0);
        GeometryFactory geomFactory=new GeometryFactory(precisionModel);
        WKBReader decoder=new WKBReader(geomFactory);
        Geometry polygon=null;
        try {
            polygon=decoder.read(geom);
            if(polygon==null || polygon.isEmpty()) {
                return null;
            }
        } catch (ParseException e) {
            throw new IllegalStateException(e);
        }
        
        return polygon;
    }
    public void setThemes(final List<ThemeType> themes, List<String> topicCodes) {
        for(String theme:topicCodes) {
            ThemeType themeEle = createTheme(theme);
            themes.add(themeEle);
        }
    }

    private ThemeType createTheme(String themeCode) {
        ThemeType themeEle=new ThemeType();
        themeEle.setCode(themeCode);
        themeEle.setText(getTopicText(themeCode));
        return themeEle;
    }

    private void addRestrictions(ExtractType extract, Geometry parcelGeom,Envelope bbox,boolean withGeometry, boolean withImages,
            List<String> queryTopics, List<String> concernedTopicsList) {
        // select schnitt parcelGeom/oerebGeom where restritctionTopic in queryTopic
        WKBWriter geomEncoder=new WKBWriter(2,ByteOrderValues.BIG_ENDIAN);
        byte geom[]=geomEncoder.write(parcelGeom);
        PrecisionModel precisionModel=new PrecisionModel(1000.0);
        GeometryFactory geomFactory=new GeometryFactory(precisionModel);
        WKBReader geomDecoder=new WKBReader(geomFactory);
        
        String sqlStmt="SELECT " + 
        "g.t_id as g_id," + 
        "ea.aname_de as ea_aname_de," + 
        "ea.amtimweb as ea_amtimweb," + 
        "ea.auid as ea_auid," + 
        "ga.aname_de as ga_aname_de," + 
        "ga.amtimweb as ga_amtimweb," + 
        "ga.auid as ga_auid," + 
        "d.t_id as d_id," + 
        "d.verweiswms," + 
        "d.legendeimweb," + 
        "e.t_id as e_id," + 
        "e.aussage_de," + 
        "e.thema," + 
        "e.subthema," + 
        "e.weiteresthema," + 
        "e.artcode," + 
        "e.artcodeliste," + 
        "e.rechtsstatus as e_rechtsstatus," + 
        "e.publiziertab," + 
        "g.rechtsstatus as g_rechtsstatus," + 
        "g.publiziertab," + 
        "ST_AsBinary(g.punkt_lv95) as punkt," + 
        "ST_AsBinary(g.linie_lv95) as linie," + 
        "ST_AsBinary(g.flaeche_lv95) as flaeche," + 
        "g.metadatengeobasisdaten" + 
        " FROM "+getSchema()+".oerbkrmfr_v1_1transferstruktur_geometrie as g " + 
        " INNER JOIN "+getSchema()+".oerbkrmfr_v1_1transferstruktur_eigentumsbeschraenkung as e ON g.eigentumsbeschraenkung = e.t_id" + 
        " INNER JOIN "+getSchema()+".oerbkrmfr_v1_1transferstruktur_darstellungsdienst as d ON e.darstellungsdienst = d.t_id" + 
        " INNER JOIN "+getSchema()+".oerbkrmvs_v1_1vorschriften_amt as ea ON e.zustaendigestelle = ea.t_id"+
        " INNER JOIN "+getSchema()+".oerbkrmvs_v1_1vorschriften_amt as ga ON g.zustaendigestelle = ga.t_id"+
        " WHERE ST_DWithin(ST_GeomFromWKB(?,2056),flaeche_lv95,0.1)";
        Set<String> concernedTopics=new HashSet<String>();
        Map<Long,RestrictionOnLandownershipType> restrictions=new HashMap<Long,RestrictionOnLandownershipType>();
        jdbcTemplate.query(sqlStmt, new ResultSetExtractor<Object>() {

            @Override
            public Object extractData(ResultSet rs) throws SQLException, DataAccessException {
                while(rs.next()) {
                    long g_id=rs.getLong("g_id");
                    long e_id=rs.getLong("e_id");
                    long d_id=rs.getLong("d_id");
                    final String aussage_de = rs.getString("aussage_de");
                    logger.info("g_id{} e_id {} aussage {} ",g_id,e_id,aussage_de);
                    
                    String topic=rs.getString("thema");
                    if(!concernedTopics.contains(topic)) {
                        concernedTopics.add(topic);
                    }
                    RestrictionOnLandownershipType rest=restrictions.get(e_id);
                    if(rest==null) {
                        
                        RestrictionOnLandownershipType localRest=new RestrictionOnLandownershipType();
                        rest=localRest;
                        restrictions.put(e_id,rest);
                        rest.setInformation(createMultilingualMTextType(aussage_de));
                        rest.setLawstatus(mapLawstatus(rs.getString("e_rechtsstatus")));
                        ThemeType themeEle = createTheme(topic);
                        rest.setTheme(themeEle);
                        //rest.setSubTheme(value);
                        String typeCode=rs.getString("artcode"); 
                        String typeCodelist=rs.getString("artcodeliste"); 
                        rest.setTypeCode(typeCode);
                        rest.setTypeCodelist(typeCodelist);
                        
                        OfficeType zustaendigeStelle=new OfficeType();
                        zustaendigeStelle.setName(createMultilingualTextType(rs.getString("ea_aname_de")));
                        zustaendigeStelle.setOfficeAtWeb(createWebReferenceType(rs.getString("ea_amtimweb")));
                        zustaendigeStelle.setUID(rs.getString("ea_auid"));
                        rest.setResponsibleOffice(zustaendigeStelle);
                        
                        MapType map=new MapType();
                        String wmsUrl=rs.getString("verweiswms");
                        wmsUrl = getWmsUrl(bbox, wmsUrl);
                        map.setReferenceWMS(wmsUrl);
                        try {
                            byte wmsImage[]=getWmsImage(wmsUrl);
                            map.setImage(wmsImage);
                        } catch (IOException | URISyntaxException e) {
                            logger.error("failed to get wms image",e);
                            map.setImage(minimalImage);
                        }
                        map.setLayerIndex(1);
                        map.setLayerOpacity(0.6);
                        setMapBBOX(map,bbox);
                        
                        map.setLegendAtWeb(createWebReferenceType(rs.getString("legendeimweb")));
                        List<LegendEntryType> legend = map.getOtherLegend();
                        {
                            String stmt="SELECT" + 
                                    "  symbol" + 
                                    "  ,legendetext_de" + 
                                    "  ,artcode" + 
                                    "  ,artcodeliste" + 
                                    "  ,thema" + 
                                    "  ,subthema" + 
                                    "  ,weiteresthema" + 
                                    "  " + 
                                    "FROM "+getSchema()+"."+TABLE_OERBKRMFR_V1_1TRANSFERSTRUKTUR_LEGENDEEINTRAG+" WHERE oerbkrmfr_vstllngsdnst_legende=? ORDER BY t_seq";
                            jdbcTemplate.query(stmt, new RowCallbackHandler() {

                                @Override
                                public void processRow(ResultSet rs) throws SQLException {
                                    final String l_code = rs.getString("artcode");
                                    final String l_codelist = rs.getString("artcodeliste");
                                    if(l_code.equals(typeCode) && l_codelist.equals(typeCodelist)) {
                                        localRest.setSymbol(rs.getBytes("symbol"));
                                    }else {
                                        LegendEntryType l=new LegendEntryType();
                                        l.setLegendText(createMultilingualTextType(rs.getString("legendetext_de")));
                                        l.setTheme(createTheme(rs.getString("thema")));
                                        l.setSubTheme(rs.getString("subthema"));
                                        l.setSymbol(rs.getBytes("symbol"));
                                        l.setTypeCode(l_code);
                                        l.setTypeCodelist(l_codelist);
                                        legend.add(l);
                                    }
                                }
                            },d_id);
                        }
                        rest.setMap(map);
                        rest.setPartInPercent(new BigDecimal(100)); // FIXME
                        rest.setAreaShare(1); // FIXME
                        
                        String stmt="WITH RECURSIVE docs as (" + 
                                "    select cast(null as bigint) as ursprung "
                                +",ed.t_id"
                                +",ed.t_type"
                                +",ed.titel_de"
                                +",ed.offiziellertitel_de"
                                +",ed.abkuerzung_de"
                                +",ed.offiziellenr"
                                +",ed.kanton"
                                +",ed.gemeinde"
                                +",ed.dokument"
                                +",docuri1.docuri"
                                +",ed.zustaendigestelle"
                                +",ed.rechtsstatus"
                                
                                + " from "+getSchema()+"."+TABLE_OERBKRMFR_V1_1TRANSFERSTRUKTUR_HINWEISVORSCHRIFT+" as h  inner join "+getSchema()+"."+TABLE_OERBKRMVS_V1_1VORSCHRIFTEN_DOKUMENT+" as ed on h.vorschrift_oerbkrmvs_v1_1vorschriften_dokument=ed.t_id"
                                + "      INNER JOIN (SELECT "+TABLE_OEREBKRM_V1_1_MULTILINGUALURI+".oerbkrmvs_vrftn_dkment_textimweb as docid,"+TABLE_OEREBKRM_V1_1_LOCALISEDURI+".atext as docuri FROM  "+getSchema()+"."+TABLE_OEREBKRM_V1_1_MULTILINGUALURI+" INNER JOIN "+getSchema()+"."+TABLE_OEREBKRM_V1_1_LOCALISEDURI+" ON  "+TABLE_OEREBKRM_V1_1_LOCALISEDURI+".oerbkrm_v1__mltlngluri_localisedtext = "+TABLE_OEREBKRM_V1_1_MULTILINGUALURI+".t_id WHERE alanguage='de') as docuri1 ON docuri1.docid=ed.t_id"
                                +"  where eigentumsbeschraenkung=?"
                                +"    UNION ALL"  
                                +"    select w.ursprung "
                                +",wd.t_id"
                                +",wd.t_type"
                                +",wd.titel_de"
                                +",wd.offiziellertitel_de"
                                +",wd.abkuerzung_de"
                                +",wd.offiziellenr"
                                +",wd.kanton"
                                +",wd.gemeinde"
                                +",wd.dokument"
                                +",docuri2.docuri"
                                +",wd.zustaendigestelle"
                                +",wd.rechtsstatus"
                                + " from "+getSchema()+"."+TABLE_OERBKRMVS_V1_1VORSCHRIFTEN_HINWEISWEITEREDOKUMENTE+" as w  inner join "+getSchema()+"."+TABLE_OERBKRMVS_V1_1VORSCHRIFTEN_DOKUMENT+" as wd on w.hinweis=wd.t_id"
                                + "      INNER JOIN (SELECT "+TABLE_OEREBKRM_V1_1_MULTILINGUALURI+".oerbkrmvs_vrftn_dkment_textimweb as docid,"+TABLE_OEREBKRM_V1_1_LOCALISEDURI+".atext as docuri FROM  "+getSchema()+"."+TABLE_OEREBKRM_V1_1_MULTILINGUALURI+" INNER JOIN "+getSchema()+"."+TABLE_OEREBKRM_V1_1_LOCALISEDURI+" ON "+TABLE_OEREBKRM_V1_1_LOCALISEDURI+".oerbkrm_v1__mltlngluri_localisedtext = "+TABLE_OEREBKRM_V1_1_MULTILINGUALURI+".t_id WHERE alanguage='de') as docuri2 ON docuri2.docid=wd.t_id"
                                +" INNER JOIN docs as s ON s.t_id = w.ursprung" + 
                                ") SELECT * FROM docs";
                        List<DocumentBaseType> documents = rest.getLegalProvisions();
                        HashMap<Long,DocumentType> documentMap = new HashMap<Long,DocumentType>();

                        jdbcTemplate.query(stmt, new RowCallbackHandler() {

                            @Override
                            public void processRow(ResultSet rs) throws SQLException {
                                DocumentType doc=new DocumentType();
                                long docid=rs.getLong("t_id");
                                Long parentid=rs.getLong("ursprung");
                                if(rs.wasNull()) {
                                    parentid=null;
                                }
                                String type=rs.getString("t_type");
                                if(type.equals("oerbkrmvs_v1_1vorschriften_rechtsvorschrift")) {
                                    doc.setDocumentType("LegalProvision");
                                }else {
                                    doc.setDocumentType("Law");
                                //    doc.setDocumentType("Hint");
                                }
                                doc.setLawstatus(mapLawstatus(rs.getString("rechtsstatus")));
                                doc.setTitle(createMultilingualTextType(rs.getString("titel_de")));
                                doc.setOfficialTitle(createMultilingualTextType(rs.getString("offiziellertitel_de")));
                                doc.setAbbreviation(createMultilingualTextType(rs.getString("abkuerzung_de")));
                                doc.setOfficialNumber(rs.getString("offiziellenr"));
                                doc.setTextAtWeb(createMultilinualUriType(rs.getString("docuri")));
                                documentMap.put(docid,doc);
                                if(parentid==null) {
                                    documents.add(doc);
                                }else {
                                    DocumentType parent=documentMap.get(parentid);
                                    parent.getReference().add(doc);
                                }
                            }

                            
                        },e_id);

                    }
                   
                    GeometryType rGeom=new GeometryType();
                    rGeom.setLawstatus(mapLawstatus(rs.getString("g_rechtsstatus")));
                    rGeom.setMetadataOfGeographicalBaseData(rs.getString("metadatengeobasisdaten"));
                    Polygon flaeche=null;
                    try {
                        flaeche = (Polygon) geomDecoder.read(rs.getBytes("flaeche"));
                    } catch (ParseException e) {
                        throw new IllegalStateException(e);
                    }
                    SurfacePropertyTypeType flaecheGml=jts2gml.convertSurface(flaeche);
                    rGeom.setSurface(flaecheGml);
                    OfficeType zustaendigeStelle=new OfficeType();
                    zustaendigeStelle.setName(createMultilingualTextType(rs.getString("ga_aname_de")));
                    zustaendigeStelle.setOfficeAtWeb(createWebReferenceType(rs.getString("ga_amtimweb")));
                    zustaendigeStelle.setUID(rs.getString("ga_auid"));
                    rGeom.setResponsibleOffice(zustaendigeStelle);
                    rest.getGeometry().add(rGeom);
                }
                return null;
            }
            
        },geom
        );
        extract.getRealEstate().getRestrictionOnLandownership().addAll(restrictions.values());
        concernedTopicsList.addAll(concernedTopics);
    }
    protected void setMapBBOX(MapType map, Envelope bbox) {
        map.setMaxNS95(jts2gml.createPointPropertyType(new Coordinate(bbox.getMaxX(),bbox.getMaxY())));
        map.setMinNS95(jts2gml.createPointPropertyType(new Coordinate(bbox.getMinX(),bbox.getMinY())));
    }
    HashMap<String,LawstatusType> statusCodes=null;
    private static final int MAP_DPI = 300;
    private static final int MAP_WIDTH_MM = 174;
    private static final int MAP_WIDTH_PIXEL = (int) (MAP_DPI*MAP_WIDTH_MM/25.4);
    private static final int MAP_HEIGTH_MM = 99;
    private static final int MAP_HEIGHT_PIXEL = (int) (MAP_DPI*MAP_HEIGTH_MM/25.4);
    private LawstatusType mapLawstatus(String xtfTransferCode) {
        if(statusCodes==null) {
            statusCodes=new HashMap<String,LawstatusType>();
            java.util.List<java.util.Map<String,Object>> baseData=jdbcTemplate.queryForList(
                    "SELECT acode,titel_de,titel_fr,titel_it,titel_rm,titel_en FROM "+getSchema()+"."+TABLE_OEREBKRM_V1_1CODELISTENTEXT_RECHTSSTATUSTXT);
            for(java.util.Map<String,Object> status:baseData) {
                LocalisedTextType statusTxt=createLocalizedText((String)status.get("titel_de"));
                LawstatusType lawstatus=new LawstatusType();
                lawstatus.setText(statusTxt);
                final String code = (String)status.get("acode");
                if(code.equals("inKraft")) {
                    lawstatus.setCode(LawstatusCodeType.IN_FORCE);
                }else if(code.equals("laufendeAenderung")) {
                    lawstatus.setCode(LawstatusCodeType.RUNNING_MODIFICATIONS);
                }
                statusCodes.put(code,lawstatus);
            }
        }
        if(xtfTransferCode!=null) {
            return statusCodes.get(xtfTransferCode);
        }
        return null;
    }

    private void setParcel(ExtractType extract, String egrid, Grundstueck parcel,Envelope bbox, boolean withGeometry) {
        WKBWriter geomEncoder=new WKBWriter(2,ByteOrderValues.BIG_ENDIAN);
        byte geom[]=geomEncoder.write(parcel.getGeometrie());
        
        RealEstateDPRType gs = new  RealEstateDPRType();
        gs.setEGRID(egrid);
        final String nbident = parcel.getNbident();
        String canton=nbident.substring(0, 2);
        gs.setCanton(CantonCodeType.fromValue(canton));
        gs.setIdentDN(nbident);
        gs.setNumber(parcel.getNummer());
        if(false) {
            List<Object[]> gslist=jdbcTemplate.query(
                    "SELECT aname,bfsnr FROM "+getSchema()+".dm01vch24lv95dgemeindegrenzen_gemeinde g LEFT JOIN "+getSchema()+".dm01vch24lv95dgemeindegrenzen_gemeindegrenze l ON g.t_id=l.gemeindegrenze_von WHERE ST_Intersects(l.geometrie,ST_GeomFromWKB(?,2056))", new RowMapper<Object[]>() {
                        
                        @Override
                        public Object[] mapRow(ResultSet rs, int rowNum) throws SQLException {
                            Object ret[]=new Object[2];
                            ret[0]=rs.getString(1);
                            ret[1]=rs.getInt(2);
                            return ret;
                        }

                        
                    },geom);
            if(gslist==null || gslist.isEmpty()) {
                return;
            }
            String gemeindename=(String) gslist.get(0)[0];
            int bfsnr=(Integer) gslist.get(0)[1];
            gs.setFosNr(bfsnr);
            gs.setMunicipality(gemeindename);
            
        }else {
            // grundbuchkreis
            java.util.Map<String,Object> gbKreis=jdbcTemplate.queryForMap(
                    "SELECT aname,bfsnr FROM "+getSchema()+"."+TABLE_SO_G_V_0180822GRUNDBUCHKREISE_GRUNDBUCHKREIS+" WHERE nbident=?",nbident);
            gs.setSubunitOfLandRegister((String)gbKreis.get("aname"));
            gs.setFosNr((Integer)gbKreis.get("bfsnr"));
            // gemeindename
            String gemeindename=jdbcTemplate.queryForObject(
                    "SELECT aname FROM "+getSchema()+"."+TABLE_DM01VCH24LV95DGEMEINDEGRENZEN_GEMEINDE+" WHERE bfsnr=?",String.class,gs.getFosNr());
            gs.setMunicipality(gemeindename);
        }
        gs.setLandRegistryArea((int)parcel.getFlaechenmas());
        gs.setType(RealEstateTypeType.REAL_ESTATE);
        //gs.setMetadataOfGeographicalBaseData(value);
        // geometry must be set here (because xml2pdf requires it), even if is not request by service client
        MultiSurfacePropertyTypeType geomGml=jts2gml.convertMultiSurface(parcel.getGeometrie());
        gs.setLimit(geomGml);
        
        
        {
            // Planausschnitt 174 * 99 mm
            MapType planForLandregister=new MapType();
            String fixedWmsUrl = getWmsUrl(bbox, oerebPlanForLandregister);
            planForLandregister.setReferenceWMS(fixedWmsUrl);
            gs.setPlanForLandRegister(planForLandregister);
            try {
                planForLandregister.setImage(getWmsImage(fixedWmsUrl));
            } catch (IOException | URISyntaxException e) {
                logger.error("failed to get wms image",e);
                planForLandregister.setImage(minimalImage);
            }
            planForLandregister.setLayerIndex(0);
            planForLandregister.setLayerOpacity(0.6);
            setMapBBOX(planForLandregister,bbox);
        }
        {
            // Planausschnitt 174 * 99 mm
            MapType planForLandregisterMainPage=new MapType();
            String fixedWmsUrl = getWmsUrl(bbox, oerebPlanForLandregisterMainPage);
            planForLandregisterMainPage.setReferenceWMS(fixedWmsUrl);
            gs.setPlanForLandRegisterMainPage(planForLandregisterMainPage);
            try {
                planForLandregisterMainPage.setImage(getWmsImage(fixedWmsUrl));
            } catch (IOException | URISyntaxException e) {
                logger.error("failed to get wms image",e);
                planForLandregisterMainPage.setImage(minimalImage);
            }
            setMapBBOX(planForLandregisterMainPage,bbox);
        }
        extract.setRealEstate(gs);
        
    }

    private Envelope getMapBBOX(Geometry parcelGeom) {
        Envelope bbox = parcelGeom.getEnvelopeInternal();
        bbox.expandBy(0.01);
        double width=bbox.getWidth();
        double height=bbox.getHeight();
        double factor=Math.max(width/MAP_WIDTH_PIXEL,height/MAP_HEIGHT_PIXEL);
        bbox.expandBy((MAP_WIDTH_PIXEL*factor-width)/2.0, (MAP_HEIGHT_PIXEL*factor-height)/2.0);
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

    private String getWmsUrl(Envelope bbox, String url) {
        final UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(url);
        builder.replaceQueryParam("BBOX", bbox.getMinX()+","+bbox.getMinY()+","+bbox.getMaxX()+","+bbox.getMaxY());
        builder.replaceQueryParam("DPI", MAP_DPI);
        builder.replaceQueryParam("HEIGHT", MAP_HEIGHT_PIXEL);
        builder.replaceQueryParam("WIDTH", MAP_WIDTH_PIXEL);
        String fixedWmsUrl = builder.build().toUriString();
        return fixedWmsUrl;
    }


    private List<String> parseTopics(String requestedTopicsAsText) {
        if(requestedTopicsAsText==null || requestedTopicsAsText.length()==0) {
            requestedTopicsAsText="ALL";
        }
        java.util.Set<String> ret=new java.util.HashSet<String>();
        String topicsx[]=requestedTopicsAsText.split(";");
        for(String topic:topicsx) {
            if(topic.equals("ALL_FEDERAL") || topic.equals("ALL")) {
                ret.add("Nutzungsplanung");
                ret.add("ProjektierungszonenNationalstrassen");
                ret.add("BaulinienNationalstrassen");
                ret.add("ProjektierungszonenEisenbahnanlagen");
                ret.add("BaulinienEisenbahnanlagen");
                ret.add("ProjektierungszonenFlughafenanlagen");
                ret.add("BaulinienFlughafenanlagen");
                ret.add("SicherheitszonenplanFlughafen");
                ret.add("BelasteteStandorte");
                ret.add("BelasteteStandorteMilitaer");
                ret.add("BelasteteStandorteZivileFlugplaetze");
                ret.add("BelasteteStandorteOeffentlicherVerkehr");
                ret.add("Grundwasserschutzzonen");
                ret.add("Grundwasserschutzareale");
                ret.add("Laermemfindlichkeitsstufen");
                ret.add("Waldgrenzen");
                ret.add("Waldabstandslinien");
                if(topic.equals("ALL")) {
                    java.util.List<String> baseDataList=jdbcTemplate.queryForList(
                            "SELECT othercode FROM "+getSchema()+"."+TABLE_OERB_XTNX_V1_0ANNEX_THEMATXT,String.class);
                    for(String extTopic:baseDataList) {
                        ret.add(extTopic);
                    }
                }
            }else {
                ret.add(topic);
            }
            
        }
        return new ArrayList<String>(ret);
    }

    private LocalisedTextType getTopicText(String code) {
        String title_de=null;
        // cantonal code?
        if(code.indexOf('.')>-1) {
            title_de=jdbcTemplate.queryForObject(
                    "SELECT titel_de FROM "+getSchema()+"."+TABLE_OERB_XTNX_V1_0ANNEX_THEMATXT+" WHERE othercode=?",String.class,code);
        }else {
            title_de=jdbcTemplate.queryForObject(
                    "SELECT titel_de FROM "+getSchema()+"."+TABLE_OEREBKRM_V1_1CODELISTENTEXT_THEMATXT+" WHERE acode=?",String.class,code);
        }
        LocalisedTextType ret=new LocalisedTextType();
        ret.setLanguage(LanguageCodeType.DE);
        ret.setText(title_de);
        return ret;
    }

    private List<String> getTopicsOfMunicipality(int bfsNr) {
        List<String> ret=jdbcTemplate.queryForList("SELECT avalue from "+getSchema()+"."+TABLE_OEREB_EXTRACTANNEX_V1_0_CODE+" as c JOIN "+getSchema()+"."+TABLE_OERB_XTNX_V1_0ANNEX_MUNICIPALITYWITHPLRC+" as m On c.oerb_xtnx_vpltywthplrc_themes=m.t_id WHERE m.municipality=?",String.class,bfsNr);
        return ret;
    }
    private List<String> getAllTopicsOfThisCadastre() {
        List<String> ret=jdbcTemplate.queryForList("SELECT DISTINCT avalue from "+getSchema()+"."+TABLE_OEREB_EXTRACTANNEX_V1_0_CODE,String.class);
        return ret;
    }
}