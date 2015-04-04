// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1. Free to use, reuse, extend, royalty free redistribution.
// Have fun Coding! ;)


namespace Sop
{
    /// <summary>
    /// DataBlockSize enumeration.
    /// 
    /// NOTE: large block sizes were defined with intent of supporting persistence
    /// of POCOs with large blobs of data, e.g. - media. However, pls. use caution
    /// in selecting your data block size, we recommend using the smaller sizes
    /// as the "large" sizes typically generates huge File sizes with a lot of
    /// unused space. They also impact in-memory caching.
    /// 
    /// NOTE 2: SOP's extension for large blobs will be tackled in a future release.
    /// </summary>
    public enum DataBlockSize
    {
        /// <summary>
        /// Unknown block size.
        /// </summary>
        Unknown = Zero,
        /// <summary>
        /// 0 byte.
        /// </summary>
        Zero = 0,
        /// <summary>
        /// Minimum Data Block size is 512 bytes.
        /// This size is mainly dictated by the limits of Win32 file system
        /// per single readable sector on disk.
        /// </summary>
        Minimum = FiveTwelve,

        //<summary>
        //512 bytes.
        //</summary>
        FiveTwelve = 512,

        //TenTwentyFour = 1024,

        /// <summary>
        /// 2048 bytes.
        /// </summary>
        //TwentyFortyEight = 2048,

        ///// <summary>
        ///// 4096 bytes.
        ///// </summary>
        //FortyNinetySix = 4096,

        ///// <summary>
        ///// 8192 bytes.
        ///// </summary>
        //EightyOneNinetyTwo = 8192,

        ///// <summary>
        ///// 16384 bytes.
        ///// </summary>
        //SixteenThreeEightyFour = 16384,

        ///// <summary>
        ///// 32768 bytes.
        ///// </summary>
        //ThirtyTwoSevenSixtyEight = 32768,

        ///// <summary>
        ///// 65536 bytes.
        ///// </summary>
        //SixtyFiveFiveThreeSix = 65536,

        ///// <summary>
        ///// 131072 bytes.
        ///// </summary>
        //OneThreeTenSeventyTwo = 131072,

        ///// <summary>
        ///// 262144 bytes.
        ///// </summary>
        //TwoSixtyTwoOneFortyFour = 262144,

        ///// <summary>
        ///// 524288 bytes.
        ///// </summary>
        //FiveTwentyFourTwoEightyEight = 524288,

        /// <summary>
        /// Maximum is 524288 bytes.
        /// </summary>
        Maximum = FiveTwelve      //FiveTwentyFourTwoEightyEight
    }
}
