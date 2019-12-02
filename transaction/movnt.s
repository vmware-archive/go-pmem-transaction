// +build amd64

TEXT ·movnt128b(SB), $0
    MOVQ x+0(FP), BX // *dest
    MOVQ y+8(FP), AX // *src
    // movdqu %(rax), %xmm0 OR __m128i xmm0 = _mm_loadu_si128((__m128i *)src);
    BYTE $0xF3; BYTE $0x0F; BYTE $0x6F; BYTE $0x00
    MOVQ BX, AX
    //movntdq %xmm0, (%rax) OR _mm_stream_si128((__m128i *)dest, xmm0);
    BYTE $0x66; BYTE $0x0f; BYTE $0xe7; BYTE $0x00
    RET

TEXT ·movnt64b(SB), $0
    MOVQ x+0(FP), BX // BX = dest
    MOVQ y+8(FP), AX // AX = src
    MOVQ (AX), DX // DX = *src
    MOVQ BX, AX // AX = dest
    // movnti %rdx, %(rax) // *dest = DX
    BYTE $0x48; BYTE $0x0f; BYTE $0xc3; BYTE $0x10
    RET

TEXT ·movnt32b(SB), $0
    MOVQ x+0(FP), BX // BX = dest
    MOVQ y+8(FP), AX // AX = src
    MOVL (AX), DX // DX = *src lower 32-b 
    MOVQ BX, AX // AX = dest
    // movnti %edx, %(rax)
    BYTE $0x0f; BYTE $0xc3; BYTE $0x10
    RET
