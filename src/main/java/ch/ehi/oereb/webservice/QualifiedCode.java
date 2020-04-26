package ch.ehi.oereb.webservice;

public class QualifiedCode implements Comparable {
    private String codespace;
    private String code;
    public QualifiedCode(String codespace, String code) {
        super();
        this.codespace = codespace;
        this.code = code;
    }
    public String getCodespace() {
        return codespace;
    }
    public String getCode() {
        return code;
    }
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((code == null) ? 0 : code.hashCode());
        result = prime * result + ((codespace == null) ? 0 : codespace.hashCode());
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
        QualifiedCode other = (QualifiedCode) obj;
        if (code == null) {
            if (other.code != null)
                return false;
        } else if (!code.equals(other.code))
            return false;
        if (codespace == null) {
            if (other.codespace != null)
                return false;
        } else if (!codespace.equals(other.codespace))
            return false;
        return true;
    }
    @Override
    public String toString() {
        return "{" + codespace + "}[" + code + "]";
    }
    @Override
    public int compareTo(Object o) {
        if(!(o instanceof QualifiedCode)) {
            throw new IllegalArgumentException("unexpected class "+o.getClass().getName());
        }
        int ns= codespace.compareTo(((QualifiedCode)o).codespace);
        if(ns==0) {
            ns= code.compareTo(((QualifiedCode)o).code);
        }
        return ns;
    }

}
