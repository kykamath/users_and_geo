from library.classes import GeneralMethods
from library.geo import geographicConvexHull
from library.geo import getLocationFromLid

class SpotsKML:
    def __init__(self):
        import simplekml
        self.kml = simplekml.Kml()
    def addLidsWithHull(self, points, color=None, withAllPoints=False):
#        points = [list(reversed(getLocationFromLid(point))) for point in points]
#        for point in points: self.kml.newpoint(coords=[point])
#        self.kml.newpoint(coords=[points[0]])
#        pol=self.kml.newpolygon(outerboundaryis=geographicConvexHull(points))
#        pol.polystyle.color = '99'+color[1:]
#        pol.polystyle.outline = 0
        self.addLocationPointsWithHull([getLocationFromLid(point) for point in points], color, withAllPoints)
    def addLocationPointsWithHull(self, points, color=None, withAllPoints=False):
        if not color: color=GeneralMethods.getRandomColor()
        points = [list(reversed(point)) for point in points]
        if not withAllPoints: self.kml.newpoint(coords=[points[0]])
        else: [self.kml.newpoint(coords=[point]) for point in points]
        pol=self.kml.newpolygon(outerboundaryis=geographicConvexHull(points))
        pol.polystyle.color = '99'+color[1:]
        pol.polystyle.outline = 0
    def addLocationPoints(self, points, color=None): 
        from simplekml import BalloonStyle
        if not color: color=GeneralMethods.getRandomColor()
        for point in [list(reversed(point)) for point in points]:
            pnt = self.kml.newpoint(coords=[point])
            pnt.iconstyle = BalloonStyle(bgcolor=color)
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
        for locations in locationsIterator: kml.addLidsWithHull(locations)
        kml.write(outputKMLFile)