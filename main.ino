// 8086tiny for ESP32 - Ported to Arduino Framework
// Original by Adrian Cable (adrian.cable@gmail.com)
// ESP32 Port with PROGMEM optimizations

#include <Arduino.h>
#include <SPIFFS.h>
#include <FS.h>
#include <time.h>
#include <pgmspace.h>

// PROGMEM helper macros
#define PROGMEM_READ_BYTE(addr) pgm_read_byte(addr)
#define PROGMEM_READ_WORD(addr) pgm_read_word(addr)
#define PROGMEM_READ_DWORD(addr) pgm_read_dword(addr)

// ESP32 specific includes
#include <driver/timer.h>
#include <esp_task_wdt.h>

// Disable graphics for this port
#define NO_GRAPHICS

// Serial debug
#define DEBUG_SERIAL Serial

// Safe debug print macros that check if serial is available
#define DEBUG_PRINT(x)     do { if (DEBUG_SERIAL) DEBUG_SERIAL.print(x); } while(0)
#define DEBUG_PRINTLN(x)   do { if (DEBUG_SERIAL) DEBUG_SERIAL.println(x); } while(0)
#define DEBUG_PRINTF(...)  do { if (DEBUG_SERIAL) DEBUG_SERIAL.printf(__VA_ARGS__); } while(0)

// Serial output buffer for BIOS/DOS output
#define SERIAL_OUT_BUFFER_SIZE 256
static char serial_out_buffer[SERIAL_OUT_BUFFER_SIZE];
static size_t serial_out_pos = 0;
static bool serial_initialized = false;

// File system paths
#define DOS_IMAGE "/dos.img"
#define BIOS_IMAGE "/bios"

// Memory layout and settings
#ifndef RAM_SIZE
#define RAM_SIZE 0x6000    // 24 KB RAM (reduced to ensure stability)
#endif

// Memory layout and register file offsets
#define REG8_OFFSET   0x00000  // 8-bit registers (256 bytes)
#define REG16_OFFSET  0x00100  // 16-bit registers (256 bytes, overlaps with 8-bit)
#define STACK_OFFSET  0x00800  // Stack area (2KB, grows down from here)
#define VIDEO_OFFSET  0x01000  // Video memory (256 bytes, 4-byte aligned)
#define AVAIL_MEM     (RAM_SIZE - VIDEO_OFFSET - VIDEO_RAM_SIZE - 0x400)  // Leave 1KB for stack

// Ensure 4-byte alignment for all memory accesses
#define WORD_ALIGNED __attribute__((aligned(4)))

// BIOS in PROGMEM
const uint8_t bios_rom[] PROGMEM = {
    // Minimal BIOS implementation
    0xEA, 0x5B, 0xE0, 0x00, 0xF0,  // JMP F000:E05B
    // Rest of the BIOS would be loaded from SPIFFS
};

// Check if our layout fits in available RAM
#if (RAM_SIZE > 0x8000)
#error "RAM_SIZE exceeds 32KB limit"
#endif

// Global memory and registers - place in DRAM with proper alignment
DRAM_ATTR WORD_ALIGNED static uint8_t mem[RAM_SIZE];
DRAM_ATTR static uint8_t * const regs8 = mem + REG8_OFFSET;
DRAM_ATTR static uint16_t * const regs16 = (uint16_t *)(mem + REG16_OFFSET);

// Keyboard buffer - place in DRAM
#define KEYBOARD_BUFFER_SIZE 16  // Reduced from 32 to save RAM
DRAM_ATTR volatile uint8_t keyboard_buffer[KEYBOARD_BUFFER_SIZE];
DRAM_ATTR volatile uint8_t keyboard_head = 0;
DRAM_ATTR volatile uint8_t keyboard_tail = 0;
DRAM_ATTR volatile uint8_t keyboard_buffer_start = 0;  // Circular buffer start
DRAM_ATTR volatile uint8_t keyboard_buffer_end = 0;    // Circular buffer end

// Timer counter
static uint32_t timer_counter = 0;

// Graphics settings - minimal text mode (40x25 chars)
#ifdef NO_GRAPHICS
static uint16_t graphics_x = 0;
static uint16_t graphics_y = 0;
#else
// Minimal text mode (40x6 characters)
static const uint16_t graphics_x = 40;
static const uint16_t graphics_y = 6;  // Reduced from 25 to 6 lines
#endif
#define GRAPHICS_X graphics_x
#define GRAPHICS_Y graphics_y

// Disable color attributes to save memory
#define NO_COLOR_ATTRIBUTES

// Emulator timing
#define EMULATOR_CLOCK_SPEED 4772727  // ~4.77MHz (original PC XT speed)
#define TIMER_DIVIDER 80              // 80MHz / 80 = 1MHz hardware timer
#define TIMER_SCALE (TIMER_BASE_CLK / TIMER_DIVIDER)  // Convert counter value to seconds

// Function prototypes
void IRAM_ATTR onTimer();
void setup_serial();
void setup_spiffs();
void setup_timer();
void handle_serial();
void handle_serial_output();

// Global variables
hw_timer_t *timer = NULL;
volatile bool timer_irq = false;
volatile bool int8_asap = false;
volatile bool trap_flag = false;

// Watchdog timer
#define WDT_TIMEOUT 3  // seconds

// Segment offset calculation
#define SEGMENT_OFFSET(seg, off) ((uint32_t)(seg) << 4 | (off))

// Stack operations
#define PUSH(val) do { \
    regs16[REG_SP] -= 2; \
    write_mem16(SEGREG(REG_SS, REG_SP, 0), (val)); \
} while(0)

#define POP() ({ \
    uint16_t val = read_mem16(SEGREG(REG_SS, REG_SP, 0)); \
    regs16[REG_SP] += 2; \
    val; \
})

// Emulator system constants
#define IO_PORT_COUNT 0x10000
// RAM_SIZE and REGS_BASE are defined above with #ifndef guards
// Minimal video memory (256 bytes for text mode 40x6 chars, 4-byte aligned)
#ifndef VIDEO_RAM_SIZE
#define VIDEO_RAM_SIZE 0x0100  // 256 bytes
#endif

// Graphics/timer/keyboard update delays (explained later)
#ifndef GRAPHICS_UPDATE_DELAY
#define GRAPHICS_UPDATE_DELAY 360000
#endif
#define KEYBOARD_TIMER_UPDATE_DELAY 20000

// 16-bit register indices
enum {
    REG_AX, REG_CX, REG_DX, REG_BX, REG_SP, REG_BP, REG_SI, REG_DI,
    REG_ES, REG_CS, REG_SS, REG_DS, REG_FS, REG_GS, REG_IP,
    REG_ZERO = 15  // Dummy register for addressing modes
};

// Maximum number of disk drives
#define MAX_DISKS 4

// Flag update masks and types (bitfield)
#define FLAGS_UPDATE_NONE      0x00  // No flags to update
#define FLAGS_UPDATE_SZP       0x01  // Update Sign, Zero, Parity flags
#define FLAGS_UPDATE_OV        0x02  // Update Overflow flag
#define FLAGS_UPDATE_AO        0x04  // Update Auxiliary and Overflow flags
#define FLAGS_UPDATE_OC_ARITH  0x02  // Update OF, CF for arithmetic operations
#define FLAGS_UPDATE_OC_SHIFT  0x04  // Update OF, CF for shift operations
#define FLAGS_UPDATE_OC_LOGIC  0x08  // Update OF, CF for logical operations
#define FLAGS_UPDATE_AC        0x10  // Update AF (Auxiliary Flag)
#define FLAGS_UPDATE_C         0x10  // Update Carry flag (same bit as AF)
#define FLAGS_UPDATE_ALL       0x1F  // Update all flags

// Flag bits in FLAGS register
enum {
    FLAG_CF = 0,  // Carry Flag
    FLAG_PF,      // Parity Flag
    FLAG_AF,      // Auxiliary Carry Flag
    FLAG_ZF,      // Zero Flag
    FLAG_SF,      // Sign Flag
    FLAG_TF,      // Trap Flag
    FLAG_IF,      // Interrupt Enable Flag
    FLAG_DF,      // Direction Flag
    FLAG_OF       // Overflow Flag
};

// Flag update types (duplicate of above, keeping for compatibility)
// These are now defined above in the Flag update masks section

// Segment override flags
#define SEG_OVERRIDE_NONE  0
#define SEG_OVERRIDE_ES    1
#define SEG_OVERRIDE_CS    2
#define SEG_OVERRIDE_SS    3
#define SEG_OVERRIDE_DS    4
#define SEG_OVERRIDE_FS    5
#define SEG_OVERRIDE_GS    6

// 8-bit register decodes
#define REG_AL 0
#define REG_AH 1
#define REG_CL 2
#define REG_CH 3
#define REG_DL 4
#define REG_DH 5
#define REG_BL 6
#define REG_BH 7

// Lookup tables in the BIOS binary
#define TABLE_XLAT_OPCODE 8
#define TABLE_XLAT_SUBFUNCTION 9
#define TABLE_STD_FLAGS 10
#define TABLE_PARITY_FLAG 11
#define TABLE_BASE_INST_SIZE 12
#define TABLE_I_W_SIZE 13
#define TABLE_I_MOD_SIZE 14
#define TABLE_COND_JUMP_DECODE_A 15
#define TABLE_COND_JUMP_DECODE_B 16
#define TABLE_COND_JUMP_DECODE_C 17
#define TABLE_COND_JUMP_DECODE_D 18
#define TABLE_FLAGS_BITFIELDS 19

// Bitfields for TABLE_STD_FLAGS values
#define TABLE_FLAG_SZP 1
#define TABLE_FLAG_AO_ARITH 2
#define TABLE_FLAG_OC_LOGIC 4

// Helper macros

// Decode mod, r_m and reg fields in instruction
#define DECODE_RM_REG do { \
    scratch2_uint = 4 * !i_mod; \
    if (i_mod < 3) { \
        uint8_t seg_reg = seg_override_en ? seg_override : bios_table_lookup[scratch2_uint + 3][i_rm]; \
        uint8_t base_reg = bios_table_lookup[scratch2_uint][i_rm]; \
        uint8_t index_reg = bios_table_lookup[scratch2_uint + 1][i_rm]; \
        uint8_t scale = bios_table_lookup[scratch2_uint + 2][i_rm]; \
        uint32_t offset = regs16[index_reg] + scale * i_data1; \
        rm_addr = SEGREG(seg_reg, base_reg, offset); \
    } else { \
        rm_addr = GET_REG_ADDR(i_rm); \
    } \
    op_to_addr = rm_addr; \
    op_from_addr = GET_REG_ADDR(i_reg); \
    if (i_d) { \
        scratch_uint = op_from_addr; \
        op_from_addr = rm_addr; \
        op_to_addr = scratch_uint; \
    } \
} while(0)

// Return memory-mapped register location (offset into mem array) for register #reg_id
#define GET_REG_ADDR(reg_id) (REGS_BASE + (i_w ? 2 * reg_id : 2 * reg_id + reg_id / 4 & 7))

// Returns number of top bit in operand (i.e. 8 for 8-bit operands, 16 for 16-bit operands)
#define TOP_BIT 8*(i_w + 1)

// Opcode execution unit helpers
#define OPCODE ;break; case
#define OPCODE_CHAIN ; case

// [I]MUL/[I]DIV/DAA/DAS/ADC/SBB helpers
#define MUL_MACRO(op_data_type,out_regs) (set_opcode(0x10), \
                                          out_regs[i_w + 1] = (op_result = CAST(op_data_type)mem[rm_addr] * (op_data_type)*out_regs) >> 16, \
                                          regs16[REG_AX] = op_result, \
                                          set_OF(set_CF(op_result - (op_data_type)op_result)))
#define DIV_MACRO(out_data_type,in_data_type,out_regs) (scratch_int = CAST(out_data_type)mem[rm_addr]) && !(scratch2_uint = (in_data_type)(scratch_uint = (out_regs[i_w+1] << 16) + regs16[REG_AX]) / scratch_int, scratch2_uint - (out_data_type)scratch2_uint) ? out_regs[i_w+1] = scratch_uint - scratch_int * (*out_regs = scratch2_uint) : pc_interrupt(0)
#define DAA_DAS(op1,op2,mask,min) set_AF((((scratch2_uint = regs8[REG_AL]) & 0x0F) > 9) || regs8[FLAG_AF]) && (op_result = regs8[REG_AL] op1 6, set_CF(regs8[FLAG_CF] || (regs8[REG_AL] op2 scratch2_uint))), \
                                  set_CF((((mask & 1 ? scratch2_uint : regs8[REG_AL]) & mask) > min) || regs8[FLAG_CF]) && (op_result = regs8[REG_AL] op1 0x60)
#define ADC_SBB_MACRO(a) OP(a##= regs8[FLAG_CF] +), \
                         set_CF(regs8[FLAG_CF] && (op_result == op_dest) || (a op_result < a(int)op_dest)), \
                         set_AF_OF_arith()

// Execute arithmetic/logic operations in emulator memory/registers
#define R_M_OP(dest,op,src) (i_w ? (op_dest = CAST(unsigned short)dest, op_result = CAST(unsigned short)dest op (op_source = CAST(unsigned short)src)) \
    : (op_dest = (dest) & 0xFF, op_result = (dest) op (op_source = CAST(unsigned char)src)))
#define MEM_OP(dest,op,src) R_M_OP(mem[dest],op,mem[src])
#define OP(op) MEM_OP(op_to_addr,op,op_from_addr)

// Increment or decrement a register #reg_id (usually SI or DI), depending on direction flag and operand size (given by i_w)
#define INDEX_INC(reg_id) (regs16[reg_id] -= (2 * regs8[FLAG_DF] - 1)*(i_w + 1))

// Helpers for stack operations
#define R_M_PUSH(a) do { \
    i_w = 1; \
    regs16[REG_SP] -= 2; \
    uint32_t addr = SEGREG(REG_SS, REG_SP, 0); \
    R_M_OP(mem[addr], =, a); \
} while(0)
#define R_M_POP(a) do { \
    i_w = 1; \
    uint32_t addr = SEGREG(REG_SS, REG_SP, 0); \
    R_M_OP(a, =, mem[addr]); \
    regs16[REG_SP] += 2; \
} while(0)

// Convert segment:offset to linear address in emulator memory space
#define SEGREG(seg, reg_ofs, offset) (16 * regs16[seg] + regs16[reg_ofs] + (offset))

// Returns sign bit of an 8-bit or 16-bit operand
#define SIGN_OF(a) (1 & (i_w ? CAST(short)a : a) >> (TOP_BIT - 1))

// Reinterpretation cast
#define CAST(a) *(a*)&

// Keyboard driver for console. This may need changing for UNIX/non-UNIX platforms
#ifdef _WIN32
#define KEYBOARD_DRIVER kbhit() && (mem[0x4A6] = getch(), pc_interrupt(7))
#else
#define KEYBOARD_DRIVER read(0, mem + 0x4A6, 1) && (int8_asap = (mem[0x4A6] == 0x1B), pc_interrupt(7))
#endif

// Keyboard driver for SDL
#ifdef NO_GRAPHICS
#define SDL_KEYBOARD_DRIVER KEYBOARD_DRIVER
#else
#define SDL_KEYBOARD_DRIVER sdl_screen ? SDL_PollEvent(&sdl_event) && (sdl_event.type == SDL_KEYDOWN || sdl_event.type == SDL_KEYUP) && (scratch_uint = sdl_event.key.keysym.unicode, scratch2_uint = sdl_event.key.keysym.mod, CAST(short)mem[0x4A6] = 0x400 + 0x800*!!(scratch2_uint & KMOD_ALT) + 0x1000*!!(scratch2_uint & KMOD_SHIFT) + 0x2000*!!(scratch2_uint & KMOD_CTRL) + 0x4000*(sdl_event.type == SDL_KEYUP) + ((!scratch_uint || scratch_uint > 0x7F) ? sdl_event.key.keysym.sym : scratch_uint), pc_interrupt(7)) : (KEYBOARD_DRIVER)
#endif

// Global variable definitions
uint8_t io_ports[IO_PORT_COUNT];
int disk[4] = {-1, -1, -1, -1};  // Disk file handles
struct timeval tv;
time_t clock_buf;
unsigned char *opcode_stream, i_rm, i_w, i_reg, i_mod, i_mod_size, i_d, i_reg4bit, raw_opcode_id, xlat_opcode_id, extra, rep_mode, seg_override_en, rep_override_en, scratch_uchar, io_hi_lo, *vid_mem_base, spkr_en, bios_table_lookup[20][256];
unsigned short reg_ip, seg_override, file_index, wave_counter;
unsigned int op_source, op_dest, rm_addr, op_to_addr, op_from_addr, i_data0, i_data1, i_data2, scratch_uint, scratch2_uint, inst_counter, set_flags_type, vmem_ctr;
int op_result, disk_fd, scratch_int;
volatile uint32_t timer_ticks = 0;

// Initialize serial communication
void setup_serial() {
    Serial.begin(115200);
    while (!Serial && millis() < 5000); // Wait up to 5 seconds for serial
    DEBUG_PRINTLN("\n8086 Emulator for ESP32");
    DEBUG_PRINTLN("=====================");
    DEBUG_PRINT("RAM: ");
    DEBUG_PRINT(RAM_SIZE / 1024);
    DEBUG_PRINTLN(" KB");
    DEBUG_PRINT("CPU: ~");
    DEBUG_PRINT(ESP.getCpuFreqMHz());
    DEBUG_PRINTLN(" MHz");
    DEBUG_PRINTLN("=====================\n");
}

// Initialize SPIFFS and load DOS image
void setup_spiffs() {
    if (!SPIFFS.begin(true)) {
        DEBUG_PRINTLN("Error: Failed to mount SPIFFS");
        while (1);
    }
    
    // Check if BIOS and DOS images exist
    bool bios_exists = SPIFFS.exists(BIOS_IMAGE);
    bool dos_exists = SPIFFS.exists(DOS_IMAGE);
    
    if (!bios_exists || !dos_exists) {
        DEBUG_PRINTLN("Error: Required files missing in SPIFFS");
        if (!bios_exists) DEBUG_PRINTLN("- BIOS file not found: " BIOS_IMAGE);
        if (!dos_exists) DEBUG_PRINTLN("- DOS image not found: " DOS_IMAGE);
        DEBUG_PRINTLN("Please upload the required files to SPIFFS");
        while (1);
    }
    
    // Load BIOS
    File bios_file = SPIFFS.open(BIOS_IMAGE, "r");
    if (!bios_file) {
        DEBUG_PRINTLN("Error: Failed to open BIOS file");
        while (1);
    }
    
    // Read BIOS to memory at 0xF0000 (top of conventional memory)
    size_t bios_size = bios_file.size();
    if (bios_size > 0x10000) {
        DEBUG_PRINTLN("Error: BIOS file too large");
        bios_file.close();
        while (1);
    }
    
    bios_file.read(mem + 0xF0000, bios_size);
    bios_file.close();
    
    // Open the first disk (drive A:)
    File dos_file = SPIFFS.open(DOS_IMAGE, "r");
    if (dos_file) {
        disk[0] = 0;  // Use index 0 for the first disk
        dos_file.close();
    } else {
        DEBUG_PRINTLN("Error: Failed to open DOS image");
        while(1);
    }
    
    // Read boot sector to 0x7C00
    if (dos_file.read(mem + 0x7C00, 512) != 512) {
        DEBUG_PRINTLN("Error: Failed to read DOS boot sector");
        while(1);
    }
    
    DEBUG_PRINTLN("BIOS and DOS loaded successfully");
}

// Setup hardware timer for emulation
void setup_timer() {
    timer = timerBegin(0, TIMER_DIVIDER, true);
    timerAttachInterrupt(timer, &onTimer, true);
    timerAlarmWrite(timer, 1000000 / (EMULATOR_CLOCK_SPEED / 1000), true);
    timerAlarmEnable(timer);
}

// Timer interrupt handler
void IRAM_ATTR onTimer() {
    timer_irq = true;
    timer_counter++;
    
    // Check keyboard buffer periodically
    static uint8_t kb_counter = 0;
    if (++kb_counter >= 10) {  // Check every 10 timer ticks
        kb_counter = 0;
        handle_serial();
    }
}

// Handle serial input
void handle_serial() {
    if (!Serial) return;
    
    while (Serial.available() > 0) {
        uint8_t key = Serial.read();
        // Add to keyboard buffer if there's space
        uint8_t next_pos = (keyboard_buffer_end + 1) % KEYBOARD_BUFFER_SIZE;
        if (next_pos != keyboard_buffer_start) {
            keyboard_buffer[keyboard_buffer_end] = key;
            keyboard_buffer_end = next_pos;
        }
    }
    
    // Periodically flush output buffer
    static unsigned long last_flush = 0;
    if (millis() - last_flush > 100) {  // Flush every 100ms
        handle_serial_output();
        last_flush = millis();
    }
}

// Handle serial output
void handle_serial_output() {
    if (serial_out_pos > 0 && Serial) {
        Serial.write((const uint8_t*)serial_out_buffer, serial_out_pos);
        Serial.flush();
        serial_out_pos = 0;
    }
}

// Helper function to output a character to serial
void serial_putc(char c) {
    if (serial_out_pos >= SERIAL_OUT_BUFFER_SIZE - 1) {
        handle_serial_output();
    }
    serial_out_buffer[serial_out_pos++] = c;
}

// Arduino setup function
void setup() {
    // Initialize serial communication
    setup_serial();
    
    // Initialize SPIFFS and load DOS image
    setup_spiffs();
    
    // Initialize hardware timer
    setup_timer();
    
    // Initialize watchdog timer
    esp_task_wdt_init(WDT_TIMEOUT, true);
    esp_task_wdt_add(NULL);
    
    // Initialize memory and registers
    memset(mem, 0, RAM_SIZE);
    
    // Initialize CPU state
    init_cpu();
    
    // Initialize I/O ports
    memset(io_ports, 0, IO_PORT_COUNT);
    
    // Initialize PIC (Programmable Interrupt Controller)
    io_ports[0x20] = 0x1F;  // ICW1: Edge triggered, cascade, ICW4 needed
    io_ports[0x21] = 0x08;  // ICW2: Interrupt vector offset 0x08
    io_ports[0x21] = 0x04;  // ICW3: Slave on IRQ2
    io_ports[0x21] = 0x01;  // ICW4: 8086 mode, normal EOI
    io_ports[0x21] = 0xFF;  // OCW1: Mask all interrupts
    
    // Initialize PIT (Programmable Interval Timer)
    io_ports[0x43] = 0x34;  // Channel 0, mode 2, binary
    io_ports[0x40] = 0x9B;  // LSB of counter (1193182 Hz / 100 Hz = 11931 = 0x2E9B)
    io_ports[0x40] = 0x2E;  // MSB of counter
    
    // Initialize keyboard controller
    io_ports[0x60] = 0;  // Keyboard data port
    io_ports[0x64] = 0xAE;  // Keyboard controller command byte (enable keyboard interface)
    
    DEBUG_PRINTLN("Emulator initialized. Starting execution...");
}

// Initialize CPU registers and state
void init_cpu() {
    // Clear all registers
    memset(regs8, 0, 0x100);
    
    // Set initial segment registers
    regs16[REG_CS] = 0xF000;
    regs16[REG_DS] = 0x0000;
    regs16[REG_ES] = 0x0000;
    regs16[REG_SS] = 0x0000;
    
    // Set initial stack pointer (just below 0x10000)
    regs16[REG_SP] = 0xFFFE;
    
    // Set instruction pointer to start of BIOS (0xFFFF0)
    regs16[REG_IP] = 0xFFF0;
    
    // Clear flags
    regs8[FLAG_CF] = 0;
    regs8[FLAG_PF] = 0;
    regs8[FLAG_AF] = 0;
    regs8[FLAG_ZF] = 0;
    regs8[FLAG_SF] = 0;
    regs8[FLAG_TF] = 0;
    regs8[FLAG_IF] = 1;  // Enable interrupts
    regs8[FLAG_DF] = 0;
    regs8[FLAG_OF] = 0;
    
    // Initialize keyboard buffer
    keyboard_buffer_start = 0;
    keyboard_buffer_end = 0;
    
    // Initialize timer counter
    timer_counter = 0;
    
    // Initialize other CPU state
    seg_override_en = 0;
    rep_override_en = 0;
    trap_flag = 0;
    int8_asap = 0;
}

// Main emulation loop
void loop() {
    // Handle serial input
    handle_serial();
    
    // Execute a batch of instructions
    execute_instructions(1000);
    
    // Handle timer interrupts
    if (timer_irq) {
        timer_irq = false;
        // Trigger timer interrupt
        if (regs8[FLAG_IF]) {
            pc_interrupt(0x08);  // IRQ0 (timer)
        }
    }
    
    // Reset watchdog timer
    esp_task_wdt_reset();
    
    // Small delay to prevent watchdog timeout
    delay(1);
}

// Helper functions

// Calculate effective address
uint32_t calc_ea(uint8_t mod, uint8_t rm) {
    uint32_t ea = 0;
    
    switch (mod) {
        case 0:  // [reg]
            if (rm == 6) {  // Direct address
                ea = read_mem16(SEGMENT_OFFSET(regs16[REG_DS], regs16[REG_IP]));
                regs16[REG_IP] += 2;
            } else {
                // [BX+SI], [BX+DI], [BP+SI], [BP+DI], [SI], [DI], [BP], [BX]
                static const uint8_t rm_table[8] = {REG_BX, REG_BX, REG_BP, REG_BP, REG_SI, REG_DI, REG_BP, REG_BX};
                static const uint8_t rm_idx[8] = {REG_SI, REG_DI, REG_SI, REG_DI, 0xFF, 0xFF, 0xFF, 0xFF};
                
                uint8_t base_reg = rm_table[rm];
                uint8_t idx_reg = rm_idx[rm];
                
                ea = regs16[base_reg];
                if (idx_reg != 0xFF) {
                    ea += regs16[idx_reg];
                }
            }
            break;
            
        case 1:  // [reg + disp8]
        case 2:  // [reg + disp16]
            ea = calc_ea(0, rm);  // Get base address
            if (mod == 1) {
                int8_t disp8 = read_mem8(SEGMENT_OFFSET(regs16[REG_CS], regs16[REG_IP]++));
                ea += (int16_t)disp8;  // Sign extend
            } else {
                int16_t disp16 = read_mem16(SEGMENT_OFFSET(regs16[REG_CS], regs16[REG_IP]));
                regs16[REG_IP] += 2;
                ea += disp16;
            }
            break;
            
        case 3:  // Register mode - should be handled by caller
            break;
    }
    
    return ea;
}

// Parity lookup table in PROGMEM
const uint8_t parity_table[256] PROGMEM = {
    1, 0, 0, 1, 0, 1, 1, 0, 0, 1, 1, 0, 1, 0, 0, 1,
    0, 1, 1, 0, 1, 0, 0, 1, 1, 0, 0, 1, 0, 1, 1, 0,
    0, 1, 1, 0, 1, 0, 0, 1, 1, 0, 0, 1, 0, 1, 1, 0,
    1, 0, 0, 1, 0, 1, 1, 0, 0, 1, 1, 0, 1, 0, 0, 1,
    0, 1, 1, 0, 1, 0, 0, 1, 1, 0, 0, 1, 0, 1, 1, 0,
    1, 0, 0, 1, 0, 1, 1, 0, 0, 1, 1, 0, 1, 0, 0, 1,
    1, 0, 0, 1, 0, 1, 1, 0, 0, 1, 1, 0, 1, 0, 0, 1,
    0, 1, 1, 0, 1, 0, 0, 1, 1, 0, 0, 1, 0, 1, 1, 0,
    0, 1, 1, 0, 1, 0, 0, 1, 1, 0, 0, 1, 0, 1, 1, 0,
    1, 0, 0, 1, 0, 1, 1, 0, 0, 1, 1, 0, 1, 0, 0, 1,
    1, 0, 0, 1, 0, 1, 1, 0, 0, 1, 1, 0, 1, 0, 0, 1,
    0, 1, 1, 0, 1, 0, 0, 1, 1, 0, 0, 1, 0, 1, 1, 0,
    1, 0, 0, 1, 0, 1, 1, 0, 0, 1, 1, 0, 1, 0, 0, 1,
    0, 1, 1, 0, 1, 0, 0, 1, 1, 0, 0, 1, 0, 1, 1, 0,
    0, 1, 1, 0, 1, 0, 0, 1, 1, 0, 0, 1, 0, 1, 1, 0,
    1, 0, 0, 1, 0, 1, 1, 0, 0, 1, 1, 0, 1, 0, 0, 1
};

// Update flags after arithmetic operations (8-bit)
void update_flags_arith8(uint8_t res, uint8_t dst, uint8_t src, bool with_carry) {
    regs8[FLAG_CF] = (res < dst) || (with_carry && (res == dst) && regs8[FLAG_CF]);
    regs8[FLAG_PF] = parity_table[res];
    regs8[FLAG_AF] = ((dst ^ src ^ res) & 0x10) ? 1 : 0;
    regs8[FLAG_ZF] = (res == 0);
    regs8[FLAG_SF] = (res & 0x80) ? 1 : 0;
    
    // OF = (dst_sign == src_sign) && (res_sign != dst_sign)
    regs8[FLAG_OF] = ((dst ^ res) & (src ^ res) & 0x80) ? 1 : 0;
}

// Update flags after arithmetic operations (16-bit)
void update_flags_arith16(uint16_t res, uint16_t dst, uint16_t src, bool with_carry) {
    regs8[FLAG_CF] = (res < dst) || (with_carry && (res == dst) && regs8[FLAG_CF]);
    regs8[FLAG_PF] = parity_table[res & 0xFF];
    regs8[FLAG_AF] = ((dst ^ src ^ res) & 0x10) ? 1 : 0;
    regs8[FLAG_ZF] = (res == 0);
    regs8[FLAG_SF] = (res & 0x8000) ? 1 : 0;
    
    // OF = (dst_sign == src_sign) && (res_sign != dst_sign)
    regs8[FLAG_OF] = ((dst ^ res) & (src ^ res) & 0x8000) ? 1 : 0;
}

// Update flags after logical operations (8-bit)
void update_flags_logic8(uint8_t res) {
    regs8[FLAG_CF] = 0;
    regs8[FLAG_PF] = PROGMEM_READ_BYTE(&parity_table[res]);
    regs8[FLAG_AF] = 0;  // Undefined, but typically cleared
    regs8[FLAG_ZF] = (res == 0);
    regs8[FLAG_SF] = (res & 0x80) ? 1 : 0;
    regs8[FLAG_OF] = 0;
}

// Update flags after logical operations (16-bit)
void update_flags_logic16(uint16_t res) {
    regs8[FLAG_CF] = 0;
    regs8[FLAG_PF] = PROGMEM_READ_BYTE(&parity_table[res & 0xFF]);
    regs8[FLAG_AF] = 0;  // Undefined, but typically cleared
    regs8[FLAG_ZF] = (res == 0);
    regs8[FLAG_SF] = (res & 0x8000) ? 1 : 0;
    regs8[FLAG_OF] = 0;
}

// Set carry flag
char set_CF(int new_CF) {
    regs8[FLAG_CF] = new_CF ? 1 : 0;
    return regs8[FLAG_CF];
}

// Set auxiliary flag
char set_AF(int new_AF) {
    regs8[FLAG_AF] = new_AF ? 1 : 0;
    return regs8[FLAG_AF];
}

// Set overflow flag
char set_OF(int new_OF) {
    regs8[FLAG_OF] = new_OF ? 1 : 0;
    return regs8[FLAG_OF];
}

// Set auxiliary and overflow flag after arithmetic operations
void set_AF_OF_arith() {
    op_source ^= op_dest ^ op_result;
    set_AF(op_source & 0x10);
    if (op_result == op_dest) {
        set_OF(0);
    } else {
        set_OF(1 & (regs8[FLAG_CF] ^ (op_source >> (TOP_BIT - 1))));
    }
}

// Assemble and return emulated CPU FLAGS register
void make_flags() {
    scratch_uint = 0xF002; // 8086 has reserved and unused flags set to 1
    for (int i = 9; i--;) {
        scratch_uint += regs8[FLAG_CF + i] << bios_table_lookup[TABLE_FLAGS_BITFIELDS][i];
    }
}

// Set emulated CPU FLAGS register from regs8[FLAG_xx] values
void set_flags(int new_flags) {
    for (int i = 9; i--;) {
        regs8[FLAG_CF + i] = !!(1 << bios_table_lookup[TABLE_FLAGS_BITFIELDS][i] & new_flags);
    }
}

// Convert raw opcode to translated opcode index
void set_opcode(unsigned char opcode) {
    xlat_opcode_id = bios_table_lookup[TABLE_XLAT_OPCODE][raw_opcode_id = opcode];
    extra = bios_table_lookup[TABLE_XLAT_SUBFUNCTION][opcode];
    i_mod_size = bios_table_lookup[TABLE_I_MOD_SIZE][opcode];
    set_flags_type = bios_table_lookup[TABLE_STD_FLAGS][opcode];
}

// Execute INT #interrupt_num on the emulated machine
char pc_interrupt(unsigned char interrupt_num) {
    uint16_t flags;
    
    // Save current flags
    make_flags();
    flags = scratch_uint;
    
    // Push flags, CS, and IP onto the stack
    PUSH(flags);
    PUSH(regs16[REG_CS]);
    PUSH(regs16[REG_IP]);
    
    // Clear interrupt and trap flags
    regs8[FLAG_IF] = 0;
    regs8[FLAG_TF] = 0;
    
    // Get new CS:IP from interrupt vector table
    uint32_t vector_addr = 4 * interrupt_num;
    if (vector_addr + 3 >= RAM_SIZE) {
        // Invalid interrupt vector
        return 0;
    }
    
    // Read new IP and CS from interrupt vector table
    regs16[REG_IP] = read_mem16(vector_addr);
    regs16[REG_CS] = read_mem16(vector_addr + 2);
    
    return 1;
}

// Memory access functions with bounds checking and ROM protection
uint8_t IRAM_ATTR read_mem8(uint32_t addr) {
    // Check if address is in RAM
    if (addr < RAM_SIZE) {
        // Direct access with bounds checking and proper alignment
        return mem[addr];
    }
    // Check if address is in BIOS ROM area (last 64KB of address space)
    else if (addr >= 0xF0000) {
        return PROGMEM_READ_BYTE(&bios_rom[addr & 0xFFFF]);
    }
    // Trigger a general protection fault on invalid memory access
    pc_interrupt(0x0D);
    return 0xFF;
}

uint16_t IRAM_ATTR read_mem16(uint32_t addr) {
    // Check if address is in RAM
    if (addr + 1 < RAM_SIZE) {
        // Direct access with proper casting
        return *(volatile uint16_t *)((uintptr_t)mem + addr);
    }
    // Check if address is in BIOS ROM area (last 64KB of address space)
    else if (addr >= 0xF0000 && addr < 0xFFFFF) {
        return PROGMEM_READ_WORD(&bios_rom[addr & 0xFFFF]);
    }
    // Trigger a general protection fault on invalid memory access
    pc_interrupt(0x0D);
    return 0xFFFF;
}

void IRAM_ATTR write_mem8(uint32_t addr, uint8_t value) {
    // Only allow writes to RAM and only if within bounds
    if (addr < RAM_SIZE) {
        // Direct access with bounds checking
        mem[addr] = value;
    } else {
        // ROM area or invalid address
        pc_interrupt(0x0D);  // General protection fault
    }
}

void IRAM_ATTR write_mem16(uint32_t addr, uint16_t value) {
    // Only allow writes to RAM and only if within bounds
    if (addr + 1 < RAM_SIZE) {
        // Direct access with proper casting
        *(volatile uint16_t *)((uintptr_t)mem + addr) = value;
    } else {
        // Trigger a general protection fault on invalid memory access
        pc_interrupt(0x0D);
    }
}

// Read a byte from I/O port
uint8_t inb(uint16_t port) {
    // Handle common I/O ports
    switch (port) {
        case 0x60:  // Keyboard controller data
            // Read from keyboard buffer if available
            if (keyboard_buffer_start != keyboard_buffer_end) {
                uint8_t key = keyboard_buffer[keyboard_buffer_start];
                keyboard_buffer_start = (keyboard_buffer_start + 1) % KEYBOARD_BUFFER_SIZE;
                return key;
            }
            return 0;
            
        case 0x64:  // Keyboard controller status
            // Bit 0: Output buffer status (1 = data ready)
            // Bit 1: Input buffer status (1 = full)
            return (keyboard_buffer_start != keyboard_buffer_end) ? 0x01 : 0x00;
            
        case 0x20:  // PIC1 command/status
        case 0x21:  // PIC1 data
        case 0xA0:  // PIC2 command/status
        case 0xA1:  // PIC2 data
            return io_ports[port & 0xFF];
            
        default:
            // For unimplemented ports, return 0xFF
            return 0xFF;
    }
}

// Write a byte to I/O port
void outb(uint16_t port, uint8_t value) {
    // Handle common I/O ports
    switch (port) {
        case 0x20:  // PIC1 command/status
        case 0x21:  // PIC1 data
        case 0xA0:  // PIC2 command/status
        case 0xA1:  // PIC2 data
            io_ports[port & 0xFF] = value;
            break;
            
        case 0x3F8:  // COM1 data
            // Send character to serial console
            Serial.write(value);
            break;
            
        case 0x3F9:  // COM1 interrupt enable
        case 0x3FA:  // COM1 interrupt identification
        case 0x3FB:  // COM1 line control
        case 0x3FC:  // COM1 modem control
        case 0x3FD:  // COM1 line status
        case 0x3FE:  // COM1 modem status
            // Ignore for now
            break;
            
        default:
            // Ignore writes to unimplemented ports
            break;
    }
}

// Execute a batch of instructions
void execute_instructions(int count) {
    while (count-- > 0) {
        // Handle interrupts if any
        if (int8_asap && regs8[FLAG_IF]) {
            pc_interrupt(0x08);  // Timer interrupt
            int8_asap = false;
        }
        
        // Handle single-step trap
        if (trap_flag) {
            pc_interrupt(0x01);  // Single-step interrupt
            trap_flag = false;
        }
        
        // Fetch next instruction
        uint32_t addr = SEGMENT_OFFSET(regs16[REG_CS], regs16[REG_IP]++);
        if (addr >= RAM_SIZE) {
            // Handle segment limit violation
            pc_interrupt(0x0D);  // General protection fault
            continue;
        }
        
        uint8_t opcode = read_mem8(addr);
        set_opcode(opcode);
        
        // Handle the instruction
        switch (opcode) {  // Use raw opcode instead of translated one
            // Data transfer instructions
            case 0x88:  // MOV r/m8, r8
            case 0x89:  // MOV r/m16, r16
            case 0x8A:  // MOV r8, r/m8
            case 0x8B:  // MOV r16, r/m16
            case 0xB0: case 0xB1: case 0xB2: case 0xB3:  // MOV r8, imm8
            case 0xB4: case 0xB5: case 0xB6: case 0xB7:
            case 0xB8: case 0xB9: case 0xBA: case 0xBB:  // MOV r16, imm16
            case 0xBC: case 0xBD: case 0xBE: case 0xBF:
                // Handle MOV instructions
                break;
                
            // 8-bit ALU operations
            case 0x00:  // ADD r/m8, r8
            case 0x08:  // OR r/m8, r8
            case 0x10:  // ADC r/m8, r8
            case 0x18:  // SBB r/m8, r8
            case 0x20:  // AND r/m8, r8
            case 0x28:  // SUB r/m8, r8
            case 0x30:  // XOR r/m8, r8
            case 0x38:  // CMP r/m8, r8
            {
                // Decode ModR/M
                uint8_t modrm = read_mem8(SEGMENT_OFFSET(regs16[REG_CS], regs16[REG_IP]++));
                uint8_t mod = (modrm >> 6) & 0x03;
                uint8_t reg = (modrm >> 3) & 0x07;
                uint8_t rm = modrm & 0x07;
                
                uint8_t dst8, src8, res8;
                uint32_t ea;
                bool is_memory = (mod != 0x03);
                
                // Get source and destination operands
                if (is_memory) {
                    ea = calc_ea(mod, rm);
                    dst8 = read_mem8(ea);
                } else {
                    dst8 = regs8[rm];
                }
                src8 = regs8[reg];
                
                // Execute operation
                switch (opcode) {
                    case 0x00: res8 = dst8 + src8; break;  // ADD
                    case 0x08: res8 = dst8 | src8; break;  // OR
                    case 0x10: res8 = dst8 + src8 + regs8[FLAG_CF]; break;  // ADC
                    case 0x18: res8 = dst8 - src8 - regs8[FLAG_CF]; break;  // SBB
                    case 0x20: res8 = dst8 & src8; break;  // AND
                    case 0x28: res8 = dst8 - src8; break;  // SUB
                    case 0x30: res8 = dst8 ^ src8; break;  // XOR
                    case 0x38: res8 = dst8 - src8; break;  // CMP (don't store result)
                }
                
                // Store result (except for CMP)
                if (opcode != 0x38) {
                    if (is_memory) {
                        write_mem8(ea, res8);
                    } else {
                        regs8[rm] = res8;
                    }
                }
                
                // Update flags
                switch (opcode) {
                    case 0x00:  // ADD
                    case 0x10:  // ADC
                    case 0x28:  // SUB
                    case 0x38:  // CMP
                        update_flags_arith8(res8, dst8, src8, (opcode == 0x10 || opcode == 0x18));
                        break;
                    case 0x18:  // SBB
                        update_flags_arith8(res8, dst8, src8 + regs8[FLAG_CF], true);
                        break;
                    case 0x08:  // OR
                    case 0x20:  // AND
                    case 0x30:  // XOR
                        update_flags_logic8(res8);
                        break;
                }
                break;
            }
            // 16-bit ALU operations
            case 0x01:  // ADD r/m16, r16
            case 0x09:  // OR r/m16, r16
            case 0x11:  // ADC r/m16, r16
            case 0x19:  // SBB r/m16, r16
            case 0x21:  // AND r/m16, r16
            case 0x29:  // SUB r/m16, r16
            case 0x31:  // XOR r/m16, r16
            case 0x39:  // CMP r/m16, r16
            {
                // Decode ModR/M
                uint8_t modrm = read_mem8(SEGMENT_OFFSET(regs16[REG_CS], regs16[REG_IP]++));
                uint8_t mod = (modrm >> 6) & 0x03;
                uint8_t reg = (modrm >> 3) & 0x07;
                uint8_t rm = modrm & 0x07;
                
                uint16_t dst16, src16, res16;
                uint32_t ea;
                bool is_memory = (mod != 0x03);
                
                // Get source and destination operands
                if (is_memory) {
                    ea = calc_ea(mod, rm);
                    dst16 = read_mem16(ea);
                } else {
                    dst16 = regs16[rm];
                }
                src16 = regs16[reg];
                
                // Execute operation
                switch (opcode) {
                    case 0x01: res16 = dst16 + src16; break;  // ADD
                    case 0x09: res16 = dst16 | src16; break;  // OR
                    case 0x11: res16 = dst16 + src16 + regs8[FLAG_CF]; break;  // ADC
                    case 0x19: res16 = dst16 - src16 - regs8[FLAG_CF]; break;  // SBB
                    case 0x21: res16 = dst16 & src16; break;  // AND
                    case 0x29: res16 = dst16 - src16; break;  // SUB
                    case 0x31: res16 = dst16 ^ src16; break;  // XOR
                    case 0x39: res16 = dst16 - src16; break;  // CMP (don't store result)
                }
                
                // Store result (except for CMP)
                if (opcode != 0x39) {
                    if (is_memory) {
                        write_mem16(ea, res16);
                    } else {
                        regs16[rm] = res16;
                    }
                }
                
                // Update flags
                switch (opcode) {
                    case 0x01:  // ADD
                    case 0x11:  // ADC
                    case 0x29:  // SUB
                    case 0x39:  // CMP
                        update_flags_arith16(res16, dst16, src16, (opcode == 0x11 || opcode == 0x19));
                        break;
                    case 0x19:  // SBB
                        update_flags_arith16(res16, dst16, src16 + regs8[FLAG_CF], true);
                        break;
                    case 0x09:  // OR
                    case 0x21:  // AND
                    case 0x31:  // XOR
                        update_flags_logic16(res16);
                        break;
                }
                break;
            }
            // 8-bit ALU operations with immediate operands
            case 0x04:  // ADD AL, imm8
            case 0x0C:  // OR AL, imm8
            case 0x14:  // ADC AL, imm8
            case 0x1C:  // SBB AL, imm8
            case 0x24:  // AND AL, imm8
            case 0x2C:  // SUB AL, imm8
            case 0x34:  // XOR AL, imm8
            case 0x3C:  // CMP AL, imm8
            {
                uint8_t imm8 = read_mem8(SEGMENT_OFFSET(regs16[REG_CS], regs16[REG_IP]++));
                uint8_t al = regs8[REG_AL];
                uint8_t res8;
                
                switch (opcode) {
                    case 0x04: res8 = al + imm8; break;  // ADD
                    case 0x0C: res8 = al | imm8; break;  // OR
                    case 0x14: res8 = al + imm8 + regs8[FLAG_CF]; break;  // ADC
                    case 0x1C: res8 = al - imm8 - regs8[FLAG_CF]; break;  // SBB
                    case 0x24: res8 = al & imm8; break;  // AND
                    case 0x2C: res8 = al - imm8; break;  // SUB
                    case 0x34: res8 = al ^ imm8; break;  // XOR
                    case 0x3C: res8 = al - imm8; break;  // CMP (don't store result)
                    default: res8 = 0;  // Shouldn't happen
                }
                
                // Update flags
                if (opcode == 0x04 || opcode == 0x14 || opcode == 0x2C || opcode == 0x3C) {
                    // ADD, ADC, SUB, CMP - update all flags
                    update_flags_arith8(res8, al, imm8, opcode == 0x14 || opcode == 0x1C);
                } else if (opcode == 0x0C || opcode == 0x24 || opcode == 0x34) {
                    // OR, AND, XOR - update SZP, clear OF/CF
                    update_flags_logic8(res8);
                } else if (opcode == 0x1C) {
                    // SBB - update all flags
                    update_flags_arith8(res8, al, imm8 + regs8[FLAG_CF], true);
                }
                
                // Store result (except for CMP)
                if (opcode != 0x3C) {
                    regs8[REG_AL] = res8;
                }
                break;
            }
            
            // 16-bit ALU operations with immediate operands
            case 0x05:  // ADD AX, imm16
            case 0x0D:  // OR AX, imm16
            case 0x15:  // ADC AX, imm16
            case 0x1D:  // SBB AX, imm16
            case 0x25:  // AND AX, imm16
            case 0x2D:  // SUB AX, imm16
            case 0x35:  // XOR AX, imm16
            case 0x3D:  // CMP AX, imm16
            {
                uint16_t imm16 = read_mem16(SEGMENT_OFFSET(regs16[REG_CS], regs16[REG_IP]));
                regs16[REG_IP] += 2;
                uint16_t ax = regs16[REG_AX];
                uint16_t res16;
                
                switch (opcode) {
                    case 0x05: res16 = ax + imm16; break;  // ADD
                    case 0x0D: res16 = ax | imm16; break;  // OR
                    case 0x15: res16 = ax + imm16 + regs8[FLAG_CF]; break;  // ADC
                    case 0x1D: res16 = ax - imm16 - regs8[FLAG_CF]; break;  // SBB
                    case 0x25: res16 = ax & imm16; break;  // AND
                    case 0x2D: res16 = ax - imm16; break;  // SUB
                    case 0x35: res16 = ax ^ imm16; break;  // XOR
                    case 0x3D: res16 = ax - imm16; break;  // CMP (don't store result)
                    default: res16 = 0;  // Shouldn't happen
                }
                
                // Update flags
                if (opcode == 0x05 || opcode == 0x15 || opcode == 0x2D || opcode == 0x3D) {
                    // ADD, ADC, SUB, CMP - update all flags
                    update_flags_arith16(res16, ax, imm16, opcode == 0x15 || opcode == 0x1D);
                } else if (opcode == 0x0D || opcode == 0x25 || opcode == 0x35) {
                    // OR, AND, XOR - update SZP, clear OF/CF
                    update_flags_logic16(res16);
                } else if (opcode == 0x1D) {
                    // SBB - update all flags
                    update_flags_arith16(res16, ax, imm16 + regs8[FLAG_CF], true);
                }
                
                // Store result (except for CMP)
                if (opcode != 0x3D) {
                    regs16[REG_AX] = res16;
                }
                break;
            }
            case 0x06:  // PUSH ES
                PUSH(regs16[REG_ES]);
                break;
            case 0x07:  // POP ES
                regs16[REG_ES] = POP();
                break;

            case 0x0E:  // PUSH CS
                PUSH(regs16[REG_CS]);
                break;
            case 0x0F:  // POP CS (not valid in real mode)
                pc_interrupt(0x06);  // Invalid opcode
                break;
                
            // Register/memory ALU operations with immediate values
            case 0x80: case 0x81: case 0x82: case 0x83:  // ALU r/m, imm
            {
                // Decode ModR/M
                uint8_t modrm = read_mem8(SEGMENT_OFFSET(regs16[REG_CS], regs16[REG_IP]++));
                uint8_t mod = (modrm >> 6) & 0x03;
                uint8_t reg = (modrm >> 3) & 0x07;
                uint8_t rm = modrm & 0x07;
                
                bool is_8bit = (opcode & 0xFE) == 0x80;  // 0x80, 0x82 are 8-bit
                bool is_sign_extended = (opcode == 0x83);  // Sign-extended 8-bit immediate
                
                if (is_8bit || is_sign_extended) {
                    // 8-bit operation or sign-extended 8-bit immediate
                    uint8_t imm8 = read_mem8(SEGMENT_OFFSET(regs16[REG_CS], regs16[REG_IP]++));
                    int16_t imm = is_sign_extended ? (int8_t)imm8 : imm8;
                    
                    if (mod == 0x03) {
                        // Register mode
                        uint8_t *reg_ptr = &regs8[rm];
                        uint8_t dst = *reg_ptr;
                        uint8_t res8;
                        
                        switch (reg) {
                            case 0: res8 = dst + imm; break;  // ADD
                            case 1: res8 = dst | imm; break;  // OR
                            case 2: res8 = dst + imm + regs8[FLAG_CF]; break;  // ADC
                            case 3: res8 = dst - imm - regs8[FLAG_CF]; break;  // SBB
                            case 4: res8 = dst & imm; break;  // AND
                            case 5: res8 = dst - imm; break;  // SUB
                            case 6: res8 = dst ^ imm; break;  // XOR
                            case 7: res8 = dst - imm; break;  // CMP (don't store result)
                            default: res8 = 0;  // Shouldn't happen
                        }
                        
                        // Update flags
                        if (reg == 0 || reg == 2 || reg == 5 || reg == 7) {
                            // ADD, ADC, SUB, CMP - update all flags
                            update_flags_arith8(res8, dst, imm, reg == 2 || reg == 3);
                        } else if (reg == 1 || reg == 4 || reg == 6) {
                            // OR, AND, XOR - update SZP, clear OF/CF
                            update_flags_logic8(res8);
                        } else if (reg == 3) {
                            // SBB - update all flags
                            update_flags_arith8(res8, dst, imm + regs8[FLAG_CF], true);
                        }
                        
                        // Store result (except for CMP)
                        if (reg != 7) {
                            *reg_ptr = res8;
                        }
                    } else {
                        // Memory mode
                        uint32_t ea = calc_ea(mod, rm);
                        uint8_t dst = read_mem8(ea);
                        uint8_t res8;
                        
                        switch (reg) {
                            case 0: res8 = dst + imm; break;  // ADD
                            case 1: res8 = dst | imm; break;  // OR
                            case 2: res8 = dst + imm + regs8[FLAG_CF]; break;  // ADC
                            case 3: res8 = dst - imm - regs8[FLAG_CF]; break;  // SBB
                            case 4: res8 = dst & imm; break;  // AND
                            case 5: res8 = dst - imm; break;  // SUB
                            case 6: res8 = dst ^ imm; break;  // XOR
                            case 7: res8 = dst - imm; break;  // CMP (don't store result)
                            default: res8 = 0;  // Shouldn't happen
                        }
                        
                        // Update flags
                        if (reg == 0 || reg == 2 || reg == 5 || reg == 7) {
                            // ADD, ADC, SUB, CMP - update all flags
                            update_flags_arith8(res8, dst, imm, reg == 2 || reg == 3);
                        } else if (reg == 1 || reg == 4 || reg == 6) {
                            // OR, AND, XOR - update SZP, clear OF/CF
                            update_flags_logic8(res8);
                        } else if (reg == 3) {
                            // SBB - update all flags
                            update_flags_arith8(res8, dst, imm + regs8[FLAG_CF], true);
                        }
                        
                        // Store result (except for CMP)
                        if (reg != 7) {
                            write_mem8(ea, res8);
                        }
                    }
                } else {
                    // 16-bit operation
                    uint16_t imm16 = read_mem16(SEGMENT_OFFSET(regs16[REG_CS], regs16[REG_IP]));
                    regs16[REG_IP] += 2;
                    
                    if (mod == 0x03) {
                        // Register mode
                        uint16_t *reg_ptr = &regs16[rm];
                        uint16_t dst = *reg_ptr;
                        uint16_t res16;
                        
                        switch (reg) {
                            case 0: res16 = dst + imm16; break;  // ADD
                            case 1: res16 = dst | imm16; break;  // OR
                            case 2: res16 = dst + imm16 + regs8[FLAG_CF]; break;  // ADC
                            case 3: res16 = dst - imm16 - regs8[FLAG_CF]; break;  // SBB
                            case 4: res16 = dst & imm16; break;  // AND
                            case 5: res16 = dst - imm16; break;  // SUB
                            case 6: res16 = dst ^ imm16; break;  // XOR
                            case 7: res16 = dst - imm16; break;  // CMP (don't store result)
                            default: res16 = 0;  // Shouldn't happen
                        }
                        
                        // Update flags
                        if (reg == 0 || reg == 2 || reg == 5 || reg == 7) {
                            // ADD, ADC, SUB, CMP - update all flags
                            update_flags_arith16(res16, dst, imm16, reg == 2 || reg == 3);
                        } else if (reg == 1 || reg == 4 || reg == 6) {
                            // OR, AND, XOR - update SZP, clear OF/CF
                            update_flags_logic16(res16);
                        } else if (reg == 3) {
                            // SBB - update all flags
                            update_flags_arith16(res16, dst, imm16 + regs8[FLAG_CF], true);
                        }
                        
                        // Store result (except for CMP)
                        if (reg != 7) {
                            *reg_ptr = res16;
                        }
                    } else {
                        // Memory mode
                        uint32_t ea = calc_ea(mod, rm);
                        uint16_t dst = read_mem16(ea);
                        uint16_t res16;
                        
                        switch (reg) {
                            case 0: res16 = dst + imm16; break;  // ADD
                            case 1: res16 = dst | imm16; break;  // OR
                            case 2: res16 = dst + imm16 + regs8[FLAG_CF]; break;  // ADC
                            case 3: res16 = dst - imm16 - regs8[FLAG_CF]; break;  // SBB
                            case 4: res16 = dst & imm16; break;  // AND
                            case 5: res16 = dst - imm16; break;  // SUB
                            case 6: res16 = dst ^ imm16; break;  // XOR
                            case 7: res16 = dst - imm16; break;  // CMP (don't store result)
                            default: res16 = 0;  // Shouldn't happen
                        }
                        
                        // Update flags
                        if (reg == 0 || reg == 2 || reg == 5 || reg == 7) {
                            // ADD, ADC, SUB, CMP - update all flags
                            update_flags_arith16(res16, dst, imm16, reg == 2 || reg == 3);
                        } else if (reg == 1 || reg == 4 || reg == 6) {
                            // OR, AND, XOR - update SZP, clear OF/CF
                            update_flags_logic16(res16);
                        } else if (reg == 3) {
                            // SBB - update all flags
                            update_flags_arith16(res16, dst, imm16 + regs8[FLAG_CF], true);
                        }
                        
                        // Store result (except for CMP)
                        if (reg != 7) {
                            write_mem16(ea, res16);
                        }
                    }
                }
                break;
            }
                
            // String operations (handled by main instruction decoder)
            case 0xA4:  // MOVSB
            case 0xA5:  // MOVSW
            case 0xA6:  // CMPSB
            case 0xA7:  // CMPSW
            case 0xAA:  // STOSB
            case 0xAB:  // STOSW
            case 0xAC:  // LODSB
            case 0xAD:  // LODSW
            case 0xAE:  // SCASB
            case 0xAF:  // SCASW
                break;
                
            // Control transfer instructions
            case 0xE8:  // CALL rel16
            case 0xE9:  // JMP rel16
            case 0xEB:  // JMP rel8
                // Handle control transfers
                break;
                
            // I/O instructions
            case 0xE4:  // IN AL, imm8
            case 0xE5:  // IN AX, imm8
            case 0xE6:  // OUT imm8, AL
            case 0xE7:  // OUT imm8, AX
            case 0xEC:  // IN AL, DX
            case 0xED:  // IN AX, DX
            case 0xEE:  // OUT DX, AL
            case 0xEF:  // OUT DX, AX
                // Handle I/O operations
                break;
                
            default:
                // Unknown opcode - trigger invalid opcode exception
                pc_interrupt(0x06);
                break;
        }
        
        // Update instruction pointer based on instruction length
        regs16[REG_IP] += (i_mod * (i_mod != 3) + 2 * (!i_mod && i_rm == 6)) * i_mod_size + 
                         bios_table_lookup[TABLE_BASE_INST_SIZE][raw_opcode_id] + 
                         bios_table_lookup[TABLE_I_W_SIZE][raw_opcode_id] * (i_w + 1);

        // Update CPU flags based on the last operation
        if (set_flags_type & FLAGS_UPDATE_SZP) {
            // Update Sign and Zero flags
            regs8[FLAG_SF] = (op_result >> (i_w ? 15 : 7)) & 1;
            regs8[FLAG_ZF] = (op_result == 0);
            
            // Update Parity flag (even parity)
            uint8_t p = op_result & 0xFF;
            p ^= p >> 4;
            p ^= p >> 2;
            p ^= p >> 1;
            regs8[FLAG_PF] = ~p & 1;
        }

        // Update arithmetic flags if needed
        if (set_flags_type & FLAGS_UPDATE_AO) {
            // Update Auxiliary and Overflow flags for arithmetic operations
            set_AF_OF_arith();
        }

        // Update carry flag if needed
        if (set_flags_type & FLAGS_UPDATE_C) {
            // Carry flag is set by individual instructions
        }

        // Update overflow and carry flags for logical operations
        if (set_flags_type & FLAGS_UPDATE_OC_LOGIC) {
            regs8[FLAG_CF] = 0;
            regs8[FLAG_OF] = 0;
        }

        // Update overflow and carry flags for shift/rotate operations
        if (set_flags_type & FLAGS_UPDATE_OC_SHIFT) {
            // For shift operations, OF is only defined for 1-bit shifts
            if (scratch_uint == 1) {
                regs8[FLAG_OF] = (op_result >> (i_w ? 14 : 6)) ^ (op_result >> (i_w ? 15 : 7));
            }
        }

        // Poll timer/keyboard every KEYBOARD_TIMER_UPDATE_DELAY instructions
        if (!(++inst_counter % KEYBOARD_TIMER_UPDATE_DELAY))
            int8_asap = 1;
#ifndef NO_GRAPHICS
		// Update the video graphics display every GRAPHICS_UPDATE_DELAY instructions
		if (!(inst_counter % GRAPHICS_UPDATE_DELAY))
		{
			// Video card in graphics mode?
			if (io_ports[0x3B8] & 2)
			{
				// If we don't already have an SDL window open, set it up and compute color and video memory translation tables
				if (!sdl_screen)
				{
					for (int i = 0; i < 16; i++)
						pixel_colors[i] = mem[0x4AC] ? // CGA?
							cga_colors[(i & 12) >> 2] + (cga_colors[i & 3] << 16) // CGA -> RGB332
							: 0xFF*(((i & 1) << 24) + ((i & 2) << 15) + ((i & 4) << 6) + ((i & 8) >> 3)); // Hercules -> RGB332

					for (int i = 0; i < GRAPHICS_X * GRAPHICS_Y / 4; i++)
						vid_addr_lookup[i] = i / GRAPHICS_X * (GRAPHICS_X / 8) + (i / 2) % (GRAPHICS_X / 8) + 0x2000*(mem[0x4AC] ? (2 * i / GRAPHICS_X) % 2 : (4 * i / GRAPHICS_X) % 4);

					SDL_Init(SDL_INIT_VIDEO);
					sdl_screen = SDL_SetVideoMode(GRAPHICS_X, GRAPHICS_Y, 8, 0);
					SDL_EnableUNICODE(1);
					SDL_EnableKeyRepeat(500, 30);
				}

				// Refresh SDL display from emulated graphics card video RAM
				vid_mem_base = mem + 0xB0000 + 0x8000*(mem[0x4AC] ? 1 : io_ports[0x3B8] >> 7); // B800:0 for CGA/Hercules bank 2, B000:0 for Hercules bank 1
				for (int i = 0; i < GRAPHICS_X * GRAPHICS_Y / 4; i++)
					((unsigned *)sdl_screen->pixels)[i] = pixel_colors[15 & (vid_mem_base[vid_addr_lookup[i]] >> 4*!(i & 1))];

				SDL_Flip(sdl_screen);
			}
			else if (sdl_screen) // Application has gone back to text mode, so close the SDL window
			{
				SDL_QuitSubSystem(SDL_INIT_VIDEO);
				sdl_screen = 0;
			}
			SDL_PumpEvents();
		}
#endif

		// Application has set trap flag, so fire INT 1
		if (trap_flag)
			pc_interrupt(1);

		trap_flag = regs8[FLAG_TF];

		// If a timer tick is pending, interrupts are enabled, and no overrides/REP are active,
		// then process the tick and check for new keystrokes
		if (int8_asap && !seg_override_en && !rep_override_en && regs8[FLAG_IF] && !regs8[FLAG_TF])
			pc_interrupt(0xA), int8_asap = 0, SDL_KEYBOARD_DRIVER;
	}

#ifndef NO_GRAPHICS
	SDL_Quit();
#endif
	return;
}