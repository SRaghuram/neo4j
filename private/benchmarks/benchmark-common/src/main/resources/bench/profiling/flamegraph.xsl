<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <!--empty template suppresses this attribute-->
    <xsl:template match="//*[local-name()='svg']/@height"/>
    <xsl:template match="//*[local-name()='svg']/@width"/>
    <!--identity template copies everything forward by default-->
    <xsl:template match="@*|node()">
        <xsl:copy>
            <xsl:apply-templates select="@*|node()"/>
        </xsl:copy>
    </xsl:template>
</xsl:stylesheet>