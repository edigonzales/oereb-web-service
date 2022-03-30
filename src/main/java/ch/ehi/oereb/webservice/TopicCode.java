package ch.ehi.oereb.webservice;

public class TopicCode implements Comparable {
    private String subCode;
    private String mainCode;
    public TopicCode(String themeCode,String subCode) {
        super();
        this.mainCode=themeCode;
        this.subCode=subCode;
    }

    public String getSubCode() {
        return subCode;
    }


    public String getMainCode() {
        return mainCode;
    }
    public TopicCode getMainTopic() {
        if(isSubTopic()) {
            return new TopicCode(getMainCode(),null);
        }else {
            return this;
        }
    }

    
    public boolean isSubTopic()
    {
        return subCode!=null;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((mainCode == null) ? 0 : mainCode.hashCode());
        result = prime * result + ((subCode == null) ? 0 : subCode.hashCode());
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
        if (mainCode == null) {
            if (other.mainCode != null)
                return false;
        } else if (!mainCode.equals(other.mainCode))
            return false;
        if (subCode == null) {
            if (other.subCode != null)
                return false;
        } else if (!subCode.equals(other.subCode))
            return false;
        return true;
    }

    @Override
    public int compareTo(Object o) {
        if(!(o instanceof TopicCode)) {
            throw new IllegalArgumentException("unexpected class "+o.getClass().getName());
        }
        int ret= mainCode.compareTo(((TopicCode)o).mainCode);
        if(ret==0 && subCode!=null) {
            ret= subCode.compareTo(((TopicCode)o).subCode);
        }
        return ret;
}

    @Override
    public String toString() {
        return "TopicCode[" + mainCode + (subCode!=null ? ":" + subCode:"") + "]";
    }


}
