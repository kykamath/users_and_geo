from library.classes import GeneralMethods
from library.geo import geographicConvexHull
from library.geo import getLocationFromLid

class SpotsKML:
    def __init__(self):
        import simplekml
        self.kml = simplekml.Kml()
    def addPointsWithHull(self, points, color=None):
        if not color: color=GeneralMethods.getRandomColor()
        points = [list(reversed(getLocationFromLid(point))) for point in points]
#        for point in points: self.kml.newpoint(coords=[point])
        self.kml.newpoint(coords=[points[0]])
        pol=self.kml.newpolygon(outerboundaryis=geographicConvexHull(points))
        pol.polystyle.color = '99'+color[1:]
        pol.polystyle.outline = 0
    def addLine(self, points, description=None):
        from simplekml import LineStyle
        points = [list(reversed(getLocationFromLid(point))) for point in points]
        ls = self.kml.newlinestring(coords=points)
        self.kml.newpoint(coords=[points[0]]), self.kml.newpoint(coords=[points[-1]])
        if description: ls.description = description
        ls.linestyle = LineStyle(width=3.0)
    def write(self, fileName): self.kml.save(fileName)
    @staticmethod
    def drawKMLsForSpots(locationsIterator, outputKMLFile):
        kml = SpotsKML()
        for locations in locationsIterator: kml.addPointsWithHull(locations)
        kml.write(outputKMLFile)