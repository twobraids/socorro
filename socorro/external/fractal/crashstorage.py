from sys import maxint

from PIL import Image, ImageDraw

from configman import Namespace

from socorro.external.crashstorage_base import CrashStorageBase

#==============================================================================
class FractalDimensionCrashStorage(CrashStorageBase):
    """This class sends processed crash reports to an end point reachable
    by the boto S3 library.
    """
    required_config = Namespace()
    required_config.add_option(
        'image_file_name',
        default='temp.png',
        is_argument=True,
        doc='name of the output image file',
    )
    required_config.add_option(
        'multi_image',
        default=False,
        doc='True - create one image per crash, False - all crashes into one image',
    )

    required_config.add_option(
        'image_max_x',
        default=1024,
        doc='the width of image in pixels',
    )
    required_config.add_option(
        'image_max_y',
        default=600,
        doc='the height of image in pixels',
    )
    required_config.add_option(
        'line_width',
        default=1,
        doc='the stroke width of lines',
    )
    required_config.add_option(
        'lr_number_of_points',
        default=3,
        doc='number of points used in linear regression',
    )

    #--------------------------------------------------------------------------
    def scale(self, a_number, max_number, max_scale):
        """take a_number from the range 0..max_number and scale it into
        the range 0..max_scale - integer arithmethic only"""
        return (a_number * max_scale / max_number)

    #--------------------------------------------------------------------------
    def scale_point(
        self,
        a_point,
        min_point_range,
        max_point_range,
        image_dimensions
    ):
        x, y = a_point
        min_x, min_y = min_point_range
        max_x, max_y = max_point_range

        image_max_x, image_max_y = image_dimensions

        translated_x = x - min_x
        translated_y = y - min_y
        scaled_x = self.scale(translated_x, max_x - min_x, image_max_x)
        scaled_y = self.scale(translated_y, max_y - min_y, image_max_y)

        return (scaled_x, scaled_y)

    #--------------------------------------------------------------------------
    def create_image(self, image_name, points, max_y, min_x, max_x):
        image = Image.new("RGB", self.image_size)
        draw_image = ImageDraw.Draw(image)
        for a_point in points:
            image_point = self.scale_point(
                a_point,
                (min_x, 0),
                (max_x, max_y),
                self.image_size
            )
            draw_image.point(image_point, fill='white')
        image.save(image_name, format="PNG")

    #--------------------------------------------------------------------------
    def accumulate_points(self, points, max_y, min_x, max_x):
        self.points.extend(points)
        if max_x > self.max_x:
            self.max_x = max_x
        if min_x < self.min_x:
            self.min_x = min_x
        if max_y > self.max_y:
            self.max_y = max_y

    #--------------------------------------------------------------------------
    def __init__(self, config, quit_check_callback=None):
        super(FractalDimensionCrashStorage, self).__init__(
            config,
            quit_check_callback
        )
        self.image_size = (config.image_max_x, config.image_max_y)
        self.points = []
        self.min_y = 0
        self.max_y = 0
        self.min_x = maxint
        self.max_x = 0

    #--------------------------------------------------------------------------
    @staticmethod
    def get_thread_stack(a_processed_crash):
        crashed_thread_number = int(a_processed_crash
            ['json_dump']["crash_info"]["crashing_thread"])
        return a_processed_crash \
               ['json_dump']['threads'][crashed_thread_number]['frames']

    #--------------------------------------------------------------------------
    @staticmethod
    def convert_frame_to_points(frames):
        max_frames = 0
        min_offset = maxint
        max_offset = 0
        points = []
        for a_frame in frames:
            y = a_frame['frame']
            x = int(a_frame['offset'][2:], 16)
            if x > max_offset:
                max_offset = x
            if x < min_offset:
                min_offset = x
            if y > max_frames:
                max_frames = y
            points.append((x, y))
        return points, max_frames, min_offset, max_offset

    def close(self):
        self.create_image(
            self.config.image_file_name,
            self.points,
            self.max_y,
            self.min_x,
            self.max_x
        )

    #--------------------------------------------------------------------------
    def save_processed(self, a_processed_crash):
        frames = self.get_thread_stack(a_processed_crash)

        points, max_frames, min_offset, max_offset = self. convert_frame_to_points(frames)

        if self.config.multi_image:
            self.create_image(
                processed_crash['crash_id'],
                points,
                max_frames,
                min_offset,
                max_offset
            )
        else:
            self.accumulate_points(
                points,
                max_frames,
                min_offset,
                max_offset
            )

