package ch.ehi.oereb.webservice;

public class TopicCode implements Comparable {
    private String code;
    private String mainCode;
    private boolean subTopic=false;
    public static final String TODO_REMOVE_FEDERAL_TOPICS_DATA[]=new String[] {
            "Nutzungsplanung"
            ,"ProjektierungszonenNationalstrassen"
            ,"BaulinienNationalstrassen"
            ,"ProjektierungszonenEisenbahnanlagen"
            ,"BaulinienEisenbahnanlagen"
            ,"ProjektierungszonenFlughafenanlagen"
            ,"BaulinienFlughafenanlagen"
            ,"SicherheitszonenplanFlughafen"
            ,"BelasteteStandorte"
            ,"BelasteteStandorteMilitaer"
            ,"BelasteteStandorteZivileFlugplaetze"
            ,"BelasteteStandorteOeffentlicherVerkehr"
            ,"Grundwasserschutzzonen"
            ,"Grundwasserschutzareale"
            ,"Laermemfindlichkeitsstufen"
            ,"Waldgrenzen"
            ,"Waldabstandslinien"
            
    };
    public static final String WEITERES_THEMA = "WeiteresThema";
    public static final String FEDERAL_TOPICS_EXTRACT[]=new String[] {
            "ch.LandUsePlans"
            , "ch.MotorwaysProjectPlaningZones"
            , "ch.MotorwaysBuildingLines"
            , "ch.RailwaysProjectPlanningZones"
            , "ch.RailwaysBuildingLines"
            , "ch.AirportsProjectPlanningZones"
            , "ch.AirportsBuildingLines"
            , "ch.AirportsSecurityZonePlans"
            , "ch.ContaminatedSites"
            , "ch.ContaminatedMilitarySites"
            , "ch.ContaminatedCivilAviationSites"
            , "ch.ContaminatedPublicTransportSites"
            , "ch.GroundwaterProtectionZones"
            , "ch.GroundwaterProtectionSites"
            , "ch.NoiseSensitivityLevels"
            , "ch.ForestPerimeters"
            , "ch.ForestDistanceLines"
    };
    public TopicCode(String themeCode,String subCode,String otherCode) {
        super();
        if(subCode==null && otherCode==null) {
            mainCode=themeCode;
            code=themeCode;
            subTopic=false;
        }else if(otherCode!=null) {
            mainCode=WEITERES_THEMA;
            code=otherCode;
            subTopic=false;
        }else{
            mainCode=themeCode;
            code=subCode;
            subTopic=true;
        }
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((code == null) ? 0 : code.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TopicCode other = (TopicCode) obj;
        if (code == null) {
            if (other.code != null)
                return false;
        } else if (!code.equals(other.code))
            return false;
        return true;
    }


    public String getCode() {
        return code;
    }


    public String getMainCode() {
        return mainCode;
    }
    public TopicCode getMainTopic() {
        if(isSubTopic()) {
            return new TopicCode(getMainCode(),null,null);
        }else {
            return this;
        }
    }

    
    public boolean isSubTopic()
    {
        return subTopic;
    }


    @Override
    public int compareTo(Object o) {
        if(!(o instanceof TopicCode)) {
            throw new IllegalArgumentException("unexpected class "+o.getClass().getName());
        }
        return code.compareTo(((TopicCode)o).code);
    }


}
