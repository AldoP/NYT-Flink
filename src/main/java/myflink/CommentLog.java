package myflink;

import java.util.Date;

public class CommentLog implements Comparable{

    private Long approveDate;
    public String articleID;
    private Integer articleWordCount;
    private String commentID;
    private String commentType; //TODO: enum
    private Long createDate;
    private Integer depth; //TODO: piu piccolo
    private Boolean editorsSelection;
    private String inReplyTo;
    private String parentUserDisplayName;
    private Integer recommendations;
    private String sectionName;
    private String userDisplayName;
    private String userID;
    private String userLocation;

    public CommentLog() {

    }

    public CommentLog(Long approveDate, String articleID, Integer articleWordCount, String commentID, String commentType, Long createDate, Integer depth, Boolean editorsSelection, String inReplyTo, String parentUserDisplayName, Integer recommendations, String sectionName, String userDisplayName, String userID, String userLocation) {
        this.approveDate = approveDate;
        this.articleID = articleID;
        this.articleWordCount = articleWordCount;
        this.commentID = commentID;
        this.commentType = commentType;
        this.createDate = createDate;
        this.depth = depth;
        this.editorsSelection = editorsSelection;
        this.inReplyTo = inReplyTo;
        this.parentUserDisplayName = parentUserDisplayName;
        this.recommendations = recommendations;
        this.sectionName = sectionName;
        this.userDisplayName = userDisplayName;
        this.userID = userID;
        this.userLocation = userLocation;
    }

    public CommentLog(String approveDate, String articleID, String articleWordCount, String commentID, String commentType, String createDate, String depth, String editorsSelection, String inReplyTo, String parentUserDisplayName, String recommendations, String sectionName, String userDisplayName, String userID, String userLocation) {
        try {
            this.approveDate = Long.parseLong(approveDate);
            this.articleID = articleID;
            this.articleWordCount = Integer.parseInt(articleWordCount);
            this.commentID = commentID;
            this.commentType = commentType;
            this.createDate = Long.parseLong(createDate);
            this.depth = Integer.parseInt(depth);
            this.editorsSelection = Boolean.parseBoolean(editorsSelection);
            this.inReplyTo = inReplyTo;
            this.parentUserDisplayName = parentUserDisplayName;
            this.recommendations = Integer.parseInt(recommendations);
            this.sectionName = sectionName;
            this.userDisplayName = userDisplayName;
            this.userID = userID;
            this.userLocation = userLocation;
        }
        catch (Exception e){
            System.err.println("Error parsing data");
        }

    }

    public Long getApproveDate() {
        return approveDate;
    }

    public String getArticleID() {
        return articleID;
    }

    public Integer getArticleWordCount() {
        return articleWordCount;
    }

    public String getCommentID() {
        return commentID;
    }

    public String getCommentType() {
        return commentType;
    }

    public Long getCreateDate() {
        return createDate;
    }

    public Integer getDepth() {
        return depth;
    }

    public Boolean getEditorsSelection() {
        return editorsSelection;
    }

    public String getInReplyTo() {
        return inReplyTo;
    }

    public String getParentUserDisplayName() {
        return parentUserDisplayName;
    }

    public Integer getRecommendations() {
        return recommendations;
    }

    public String getSectionName() {
        return sectionName;
    }

    public String getUserDisplayName() {
        return userDisplayName;
    }

    public String getUserID() {
        return userID;
    }

    public String getUserLocation() {
        return userLocation;
    }

    public void setApproveDate(Long approveDate) {
        this.approveDate = approveDate;
    }

    public void setArticleID(String articleID) {
        this.articleID = articleID;
    }

    public void setArticleWordCount(Integer articleWordCount) {
        this.articleWordCount = articleWordCount;
    }

    public void setCommentID(String commentID) {
        this.commentID = commentID;
    }

    public void setCommentType(String commentType) {
        this.commentType = commentType;
    }

    public void setCreateDate(Long createDate) {
        this.createDate = createDate;
    }

    public void setDepth(Integer depth) {
        this.depth = depth;
    }

    public void setEditorsSelection(Boolean editorsSelection) {
        this.editorsSelection = editorsSelection;
    }

    public void setInReplyTo(String inReplyTo) {
        this.inReplyTo = inReplyTo;
    }

    public void setParentUserDisplayName(String parentUserDisplayName) {
        this.parentUserDisplayName = parentUserDisplayName;
    }

    public void setRecommendations(Integer recommendations) {
        this.recommendations = recommendations;
    }

    public void setSectionName(String sectionName) {
        this.sectionName = sectionName;
    }

    public void setUserDisplayName(String userDisplayName) {
        this.userDisplayName = userDisplayName;
    }

    public void setUserID(String userID) {
        this.userID = userID;
    }

    public void setUserLocation(String userLocation) {
        this.userLocation = userLocation;
    }


    @Override
    public String toString() {
        Date approveDateAsDate = new Date(approveDate);
        Date createDateAsDate = new Date(createDate);
        return "CommentLog{" +
                "approveDate=" + approveDateAsDate + " as long: "+approveDate+
                ", create Data= " +createDateAsDate + " as long: "+createDate+
                ", articleID='" + articleID + '\'' +
                ", userLocation='" + userLocation + '\'' +
                '}';
    }


    public static CommentLog fromString(String s) {
        String otherThanQuote = " [^\"] ";
        String quotedString = String.format(" \" %s* \" ", otherThanQuote);
        String regex = String.format("(?x) "+ // enable comments, ignore white spaces
                        ",                         "+ // match a comma
                        "(?=                       "+ // start positive look ahead
                        "  (?:                     "+ // start non-capturing group 1
                        "    %s*                   "+ // match 'otherThanQuote' zero or more times
                        "    %s                    "+ // match 'quotedString'
                        "  )*                      "+ // end group 1 and repeat it zero or more times
                        "  %s*                     "+ // match 'otherThanQuote'
                        "  $                       "+ // match the end of the string
                        ")                         ", // stop positive look ahead
                otherThanQuote, quotedString, otherThanQuote);

        String[] tokens = s.split(regex, -1);
        CommentLog commentLog = new CommentLog();

        try {
        commentLog.approveDate = Long.parseLong(tokens[0])*1000;
        commentLog.articleID = tokens[1];
        commentLog.articleWordCount = Integer.parseInt(tokens[2]);
        commentLog.commentID = tokens[3];
        commentLog.commentType = tokens[4];
        commentLog.createDate = Long.parseLong(tokens[5])*1000;
        commentLog.depth = Integer.parseInt(tokens[6]);
        commentLog.editorsSelection = Boolean.parseBoolean(tokens[7]);
        commentLog.inReplyTo = tokens[8];
        commentLog.parentUserDisplayName = tokens[9];
        commentLog.recommendations = Integer.parseInt(tokens[10]);
        commentLog.sectionName = tokens[11];
        commentLog.userDisplayName = tokens[12];
        commentLog.userID = tokens[13];
        commentLog.userLocation = tokens[14];
        } catch (Exception e) {
            System.err.println(e);
            System.err.println("Errore nel parsing");
        }

        return commentLog;
    }




    @Override
    public int compareTo(Object my_log) {
        Long compareLog = ((CommentLog)my_log).getApproveDate();
        /* For Ascending order*/
        int n = compareLog.intValue() - this.approveDate.intValue();
        return n;

        /* For Descending order do like this */
        //return compareage-this.studentage;
    }
}
