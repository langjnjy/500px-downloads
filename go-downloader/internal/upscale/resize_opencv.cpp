#include <opencv2/imgcodecs.hpp>
#include <opencv2/imgproc.hpp>

extern "C" int upscale_cubic_2x(const char *inpath, const char *outpath) {
	try {
		cv::Mat im = cv::imread(inpath, cv::IMREAD_UNCHANGED);
		if (im.empty()) {
			return 1;
		}
		cv::Mat out;
		cv::resize(im, out, cv::Size(), 2.0, 2.0, cv::INTER_CUBIC);
		if (!cv::imwrite(outpath, out)) {
			return 2;
		}
		return 0;
	} catch (...) {
		return 3;
	}
}
